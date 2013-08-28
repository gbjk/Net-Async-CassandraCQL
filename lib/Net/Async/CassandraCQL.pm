#  You may distribute under the terms of either the GNU General Public License
#  or the Artistic License (the same terms as Perl itself)
#
#  (C) Paul Evans, 2013 -- leonerd@leonerd.org.uk

package Net::Async::CassandraCQL;

use strict;
use warnings;
use 5.010;

our $VERSION = '0.01';

use base qw( IO::Async::Protocol::Stream );

use Carp;

use Protocol::CassandraCQL qw( :opcodes :results );
use Protocol::CassandraCQL::Frame;
use Protocol::CassandraCQL::ColumnMeta;
use Protocol::CassandraCQL::Result;

use constant DEFAULT_CQL_PORT => 9042;

=head1 NAME

C<Net::Async::CassandraCQL> - use Cassandra databases with L<IO::Async> using CQL

=cut

=head1 PARAMETERS

The following named parameters may be passed ot C<new> or C<configure>:

=over 8

=item host => STRING

The hostname of the Cassandra node to connect to

=item service => STRING

Optional. The service name or port number to connect to.

=back

=cut

sub configure
{
   my $self = shift;
   my %params = @_;

   foreach (qw( host service )) {
      $self->{$_} = delete $params{$_} if exists $params{$_};
   }

   $self->SUPER::configure( %params );
}

=head1 METHODS

=cut

# function
sub _decode_result
{
   my ( $response ) = @_;

   my $result = $response->unpack_int;

   if( $result == RESULT_VOID ) {
      return Future->new->done();
   }
   elsif( $result == RESULT_ROWS ) {
      return Future->new->done( rows => Protocol::CassandraCQL::Result->new( $response ) );
   }
   elsif( $result == RESULT_SET_KEYSPACE ) {
      return Future->new->done( keyspace => $response->unpack_string );
   }
   elsif( $result == RESULT_SCHEMA_CHANGE ) {
      return Future->new->done( schema_change => [ map { $response->unpack_string } 1 .. 3 ] );
   }
   else {
      return Future->new->done( "??" => $response->bytes );
   }
}

=head2 $f = $cass->connect( %args )

Connects to the Cassandra node an send the C<OPCODE_STARTUP> message. The
returned Future will yield nothing on success.

Takes the following named arguments:

=over 8

=item host => STRING

=item service => STRING

Optional. Overrides the configured values.

=back

A host name is required, either as a named argument or as a configured value
on the object. If the service name is missing, the default CQL port will be
used instead.

=cut

sub connect
{
   my $self = shift;
   my %args = @_;

   $args{host}    //= $self->{host}    or croak "Require 'host'";
   $args{service} //= $self->{service} // DEFAULT_CQL_PORT;

   return ( $self->{connect_f} ||=
      $self->SUPER::connect( %args )->on_fail( sub { undef $self->{connect_f} } ) )
      ->and_then( sub {
         $self->startup
      });
}

sub on_read
{
   my $self = shift;
   my ( $buffref, $eof ) = @_;

   return 0 unless length $$buffref >= 8;

   my $bodylen = unpack( "x4 N", $$buffref );
   return 0 unless length $$buffref >= 8 + $bodylen;

   my ( $version, $flags, $streamid, $opcode ) = unpack( "C C C C x4", substr $$buffref, 0, 8, "" );
   my $body = substr $$buffref, 0, $bodylen, "";

   # v1 response
   die sprintf "Unexpected message version %#02x\n", $version if $version != 0x81;

   my $frame = Protocol::CassandraCQL::Frame->new( $body );

   # TODO: flags
   if( my $f = $self->{streams}[$streamid] ) {
      if( $opcode == OPCODE_ERROR ) {
         my $err     = $frame->unpack_int;
         my $message = $frame->unpack_string;
         $f->fail( "OPCODE_ERROR: $message\n", $err, $frame );
      }
      else {
         $f->done( $opcode, $frame );
      }

      undef $self->{streams}[$streamid];
   }
   else {
      print STDERR "Received a message opcode=$opcode for unknown stream $streamid\n";
   }

   return 1;
}

=head2 $f = $cass->send_message( $opcode, $frame )

Sends a message with the given opcode and L<Protocol::CassandraCQL::Frame> for
the message body. The returned Future will yield the response opcode and
frame.

  ( $reply_opcode, $reply_frame ) = $f->get

This is a low-level method; applications should instead use one of the wrapper
methods below.

=cut

sub send_message
{
   my $self = shift;
   my ( $opcode, $frame ) = @_;

   my $streams = $self->{streams} ||= [];
   my $id;
   foreach ( 1 .. $#$streams ) {
      $id = $_ and last if !defined $streams->[$_];
   }
   if( !defined $id ) {
      die "TODO: Queue" if $#$streams == 127;
      $id = @$streams;
      $id = 1 if !$id; # can't use 0
   }

   my $version = 0x01;
   my $flags   = 0;
   my $body    = $frame->bytes;
   $self->write( pack "C C C C N a*", $version, $flags, $id, $opcode, length $body, $body );

   return $streams->[$id] = $self->loop->new_future;
}

=head2 $f = $cass->startup

Sends a C<OPCODE_STARTUP> message. On success, the returned Future yields
nothing.

Normally this is not required as the C<connect> method performs it implicitly.

=cut

sub startup
{
   my $self = shift;

   $self->send_message( OPCODE_STARTUP,
      Protocol::CassandraCQL::Frame->new->pack_string_map( {
            CQL_VERSION => "3.0.0",
      } )
   )->then( sub {
      my ( $op, $response ) = @_;
      $op == OPCODE_READY or die "Expected OPCODE_READY";

      return Future->new->done;
   });
}

=head2 $f = $cass->options

Sends a C<OPCODE_OPTIONS> message. On success, the returned Future yields a
HASH reference mapping option names to ARRAY references containing valid
values.

=cut

sub options
{
   my $self = shift;

   $self->send_message( OPCODE_OPTIONS,
      Protocol::CassandraCQL::Frame->new
   )->then( sub {
      my ( $op, $response ) = @_;
      $op == OPCODE_SUPPORTED or die "Expected OPCODE_SUPPORTED";

      my %opts;
      # $response contains a multimap; short * { string, string list }
      foreach ( 1 .. $response->unpack_short ) {
         my $name = $response->unpack_string;
         $opts{$name} = $response->unpack_string_list;
      }
      return Future->new->done( \%opts );
   });
}

=head2 $f = $cass->query( $cql, $consistency )

Sends a C<OPCODE_QUERY> message. On success, the values returned from the
Future will depend on the type of response.

 ( $type, $result ) = $f->get

For type C<keyspace>, C<$result> is a string giving the name of the new
keyspace (returned from C<USE> queries).

For type C<schema_change>, C<$result> is a 3-element ARRAY reference
containing the type of change, the keyspace and the table name (returned from
C<CREATE>, C<ALTER> and C<DROP> queries).

For type C<rows>, C<$result> is an instance of
L<Protocol::CassandraCQL::Result>.

For void-returning queries such as C<INSERT>, the future returns nothing.

=cut

sub query
{
   my $self = shift;
   my ( $cql, $consistency ) = @_;

   $self->send_message( OPCODE_QUERY,
      Protocol::CassandraCQL::Frame->new->pack_lstring( $cql )
                                        ->pack_short( $consistency )
   )->then( sub {
      my ( $op, $response ) = @_;
      $op == OPCODE_RESULT or die "Expected OPCODE_RESULT";
      return _decode_result( $response );
   });
}

=head2 $f = $cass->prepare( $cql )

Sends a C<OPCODE_PREPARE> message. On success, the returned Future yields
the prepared statement ID and a L<Protocol::CassandraCQL::ColumnMeta> instance
giving the parameter names and types required to execute the query.

 ( $id, $metadata ) = $f->get

=cut

sub prepare
{
   my $self = shift;
   my ( $cql ) = @_;

   $self->send_message( OPCODE_PREPARE,
      Protocol::CassandraCQL::Frame->new->pack_lstring( $cql )
   )->then( sub {
      my ( $op, $response ) = @_;
      $op == OPCODE_RESULT or die "Expected OPCODE_RESULT";

      $response->unpack_int == RESULT_PREPARED or die "Expected RESULT_PREPARED";

      my $id = $response->unpack_short_bytes;

      my $meta = Protocol::CassandraCQL::ColumnMeta->new( $response );

      Future->new->done( $id, $meta );
   });
}

=head2 $f = $cass->execute( $id, $data, $consistency )

Sends a C<OPCODE_EXECUTE> message to execute a previously-prepared statement.
On success, the returned Future will yield results of the same form as the
C<query> method. C<$data> should contain a list of encoded byte-string values.

=cut

sub execute
{
   my $self = shift;
   my ( $id, $data, $consistency ) = @_;

   my $frame = Protocol::CassandraCQL::Frame->new
      ->pack_short_bytes( $id )
      ->pack_short( scalar @$data );
   $frame->pack_bytes( $_ ) for @$data;
   $frame->pack_short( $consistency );

   $self->send_message( OPCODE_EXECUTE, $frame )->then( sub {
      my ( $op, $response ) = @_;
      $op == OPCODE_RESULT or die "Expected OPCODE_RESULT";
      return _decode_result( $response );
   });
}

=head1 TODO

=over 8

=item *

Queue messages if all 127 available stream IDs are already consumed.

=item *

Handle OPCODE_AUTHENTICATE and OPCODE_REGISTER

=item *

Codecs for TYPE_DECIMAL, TYPE_UUID, TYPE_VARINT, TYPE_INET

=item *

Allow storing multiple Cassandra node hostnames and perform some kind of
balancing or failover of connections.

=item *

Wrap prepared statements in a higher-level object that stores ID and meta
directly, allowing encoding of data.

=item *

Allow fetching row data as list or HASH of column names; allow executing
prepared from HASH too.

=back

=cut

=head1 SPONSORS

This code was paid for by

=over 2

=item *

Perceptyx L<http://www.perceptyx.com/>

=item *

Shadowcat Systems L<http://www.shadow.cat>

=back

=head1 AUTHOR

Paul Evans <leonerd@leonerd.org.uk>

=cut

0x55AA;
