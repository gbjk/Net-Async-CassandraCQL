#  You may distribute under the terms of either the GNU General Public License
#  or the Artistic License (the same terms as Perl itself)
#
#  (C) Paul Evans, 2013 -- leonerd@leonerd.org.uk

package Net::Async::CassandraCQL::Connection;

use strict;
use warnings;
use 5.010;

our $VERSION = '0.05';

use base qw( IO::Async::Protocol::Stream );

use Carp;

use Future 0.13;

use Protocol::CassandraCQL qw( :opcodes :results :consistencies );
use Protocol::CassandraCQL::Frame;
use Protocol::CassandraCQL::ColumnMeta;
use Protocol::CassandraCQL::Result;

use constant DEFAULT_CQL_PORT => 9042;

use Net::Async::CassandraCQL;
foreach (qw( quote quote_identifier )) {
   no strict 'refs';
   *$_ = Net::Async::CassandraCQL->can( $_ );
}

=head1 NAME

C<Net::Async::CassandraCQL::Connection> - connect to a single Cassandra database node

=head1 DESCRIPTION

TODO

=cut

=head1 EVENTS

=head2 on_event $name, @args

A registered event occurred. C<@args> will depend on the event name. Each
is also available as its own event, with the name in lowercase. If the event
is not one of the types recognised below, C<@args> will contain the actual
L<Protocol::CassandraCQL::Frame> object.

=head2 on_topology_change $type, $node

The cluster topology has changed. C<$node> is a packed socket address.

=head2 on_status_change $status, $node

The node's status has changed. C<$node> is a packed socket address.

=head2 on_schema_change $type, $keyspace, $table

A keyspace or table schema has changed.

=cut

=head1 PARAMETERS

The following named parameters may be passed to C<new> or C<configure>:

=over 8

=item host => STRING

The hostname of the Cassandra node to connect to

=item service => STRING

Optional. The service name or port number to connect to.

=item username => STRING

=item password => STRING

Optional. Authentication details to use for C<PasswordAuthenticator>.

=item keyspace => STRING

Optional. If set, a C<USE keyspace> query will be issued as part of the
connect method.

=back

=cut

sub _init
{
   my $self = shift;
   $self->SUPER::_init( @_ );

   $self->{streams} = []; # map [1 .. 127] to Future
   $self->{pending} = []; # queue of [$opcode, $frame, $f]
}

sub configure
{
   my $self = shift;
   my %params = @_;

   foreach (qw( host service username password keyspace
                on_event on_topology_change on_status_change on_schema_change )) {
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
      return Future->new->done( rows => Protocol::CassandraCQL::Result->from_frame( $response ) );
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

=head2 $f = $conn->connect( %args )

Connects to the Cassandra node an send the C<OPCODE_STARTUP> message. The
returned Future will yield nothing on success.

Takes the following named arguments:

=over 8

=item host => STRING

=item service => STRING

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

   my ( $version, $flags, $streamid, $opcode, $frame ) =
      Protocol::CassandraCQL::Frame->parse( $$buffref ) or return 0;

   # v1 response
   $version == 0x81 or
      $self->fail_all_and_close( sprintf "Unexpected message version %#02x\n", $version ), return;

   # TODO: flags
   if( my $f = $self->{streams}[$streamid] ) {
      undef $self->{streams}[$streamid];

      if( $opcode == OPCODE_ERROR ) {
         my $err     = $frame->unpack_int;
         my $message = $frame->unpack_string;
         $f->fail( "OPCODE_ERROR: $message\n", $err, $frame );
      }
      else {
         $f->done( $opcode, $frame );
      }

      if( my $next = shift @{ $self->{pending} } ) {
         my ( $opcode, $frame, $f ) = @$next;
         $self->_send( $opcode, $streamid, $frame, $f );
      }
   }
   elsif( $streamid == 0 and $opcode == OPCODE_ERROR ) {
      my $err     = $frame->unpack_int;
      my $message = $frame->unpack_string;
      $self->fail_all_and_close( "OPCODE_ERROR: $message\n", $err, $frame );
   }
   elsif( $streamid == 0xff and $opcode == OPCODE_EVENT ) {
      $self->_event( $frame );
   }
   else {
      print STDERR "Received a message opcode=$opcode for unknown stream $streamid\n";
   }

   return 1;
}

sub _event
{
   my $self = shift;
   my ( $frame ) = @_;

   my $name = $frame->unpack_string;

   my @args;
   if( $name eq "TOPOLOGY_CHANGE" ) {
      push @args, $frame->unpack_string; # type
      push @args, $frame->unpack_inet;   # node
   }
   elsif( $name eq "STATUS_CHANGE" ) {
      push @args, $frame->unpack_string; # status
      push @args, $frame->unpack_inet;   # node
   }
   elsif( $name eq "SCHEMA_CHANGE" ) {
      push @args, $frame->unpack_string; # type
      push @args, $frame->unpack_string; # keyspace
      push @args, $frame->unpack_string; # table
   }
   else {
       push @args, $frame;
   }

   $self->maybe_invoke_event( "on_".lc($name), @args )
      or $self->maybe_invoke_event( on_event => $name, @args );
}

sub on_closed
{
   my $self = shift;
   $self->fail_all( "Connection closed" );
}

sub fail_all
{
   my $self = shift;
   my ( $failure ) = @_;

   foreach ( @{ $self->{streams} } ) {
      $_->fail( $failure ) if $_;
   }
   @{ $self->{streams} } = ();

   foreach ( @{ $self->{pending} } ) {
      $_->[2]->fail( $failure );
   }
   @{ $self->{pending} } = ();
}

sub fail_all_and_close
{
   my $self = shift;
   my ( $failure ) = @_;

   $self->fail_all( $failure );

   $self->close;

   return Future->new->fail( $failure );
}

=head2 $f = $conn->send_message( $opcode, $frame )

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

   my $f = $self->loop->new_future;

   my $streams = $self->{streams} ||= [];
   my $id;
   foreach ( 1 .. $#$streams ) {
      $id = $_ and last if !defined $streams->[$_];
   }

   if( !defined $id ) {
      if( $#$streams == 127 ) {
         push @{ $self->{pending} }, [ $opcode, $frame, $f ];
         return $f;
      }
      $id = @$streams;
      $id = 1 if !$id; # can't use 0
   }

   $self->_send( $opcode, $id, $frame, $f );

   return $f;
}

sub _send
{
   my $self = shift;
   my ( $opcode, $id, $frame, $f ) = @_;

   $self->write( $frame->build( 0x01, 0, $id, $opcode ) );

   $self->{streams}[$id] = $f;
}

=head2 $f = $conn->startup

Sends the initial connection setup message. On success, the returned Future
yields nothing.

Normally this is not required as the C<connect> method performs it implicitly.

=cut

sub startup
{
   my $self = shift;

   $self->send_message( OPCODE_STARTUP,
      Protocol::CassandraCQL::Frame->new->pack_string_map( {
            CQL_VERSION => "3.0.5",
      } )
   )->then( sub {
      my ( $op, $response ) = @_;

      if( $op == OPCODE_READY ) {
         return Future->new->done;
      }
      elsif( $op == OPCODE_AUTHENTICATE ) {
         return $self->_authenticate( $response->unpack_string );
      }
      else {
         return $self->fail_all_and_close( "Expected OPCODE_READY or OPCODE_AUTHENTICATE" );
      }
   });
}

sub _authenticate
{
   my $self = shift;
   my ( $authenticator ) = @_;

   if( $authenticator eq "org.apache.cassandra.auth.PasswordAuthenticator" ) {
      foreach (qw( username password )) {
         defined $self->{$_} or croak "Cannot authenticate by password without $_";
      }

      $self->send_message( OPCODE_CREDENTIALS,
         Protocol::CassandraCQL::Frame->new->pack_string_map( {
            username => $self->{username},
            password => $self->{password},
         }
      )
      )->then( sub {
         my ( $op, $response ) = @_;
         $op == OPCODE_READY or return $self->fail_all_and_close( "Expected OPCODE_READY" );

         return Future->new->done;
      });
   }
   else {
      return $self->fail_all_and_close( "Unrecognised authenticator $authenticator" );
   }
}

=head2 $f = $conn->options

Requests the list of supported options from the server node. On success, the
returned Future yields a HASH reference mapping option names to ARRAY
references containing valid values.

=cut

sub options
{
   my $self = shift;

   $self->send_message( OPCODE_OPTIONS,
      Protocol::CassandraCQL::Frame->new
   )->then( sub {
      my ( $op, $response ) = @_;
      $op == OPCODE_SUPPORTED or return Future->new->fail( "Expected OPCODE_SUPPORTED" );

      my %opts;
      # $response contains a multimap; short * { string, string list }
      foreach ( 1 .. $response->unpack_short ) {
         my $name = $response->unpack_string;
         $opts{$name} = $response->unpack_string_list;
      }
      return Future->new->done( \%opts );
   });
}

=head2 $f = $conn->query( $cql, $consistency )

Performs a CQL query. On success, the values returned from the Future will
depend on the type of query.

 ( $type, $result ) = $f->get

For C<USE> queries, the type is C<keyspace> and C<$result> is a string giving
the name of the new keyspace.

For C<CREATE>, C<ALTER> and C<DROP> queries, the type is C<schema_change> and
C<$result> is a 3-element ARRAY reference containing the type of change, the
keyspace and the table name.

For C<SELECT> queries, the type is C<rows> and C<$result> is an instance of
L<Protocol::CassandraCQL::Result> containing the returned row data.

For other queries, such as C<INSERT>, C<UPDATE> and C<DELETE>, the future
returns nothing.

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
      $op == OPCODE_RESULT or return Future->new->fail( "Expected OPCODE_RESULT" );
      return _decode_result( $response );
   });
}

=head2 $f = $conn->prepare( $cql )

Prepares a CQL query for later execution. On success, the returned Future
yields the message frame of the result, after removing the C<RESULT_PREPARED>
short.

 ( $frame ) = $f->get

=cut

sub prepare
{
   my $self = shift;
   my ( $cql ) = @_;

   $self->send_message( OPCODE_PREPARE,
      Protocol::CassandraCQL::Frame->new->pack_lstring( $cql )
   )->then( sub {
      my ( $op, $response ) = @_;
      $op == OPCODE_RESULT or return Future->new->fail( "Expected OPCODE_RESULT" );

      $response->unpack_int == RESULT_PREPARED or return Future->new->fail( "Expected RESULT_PREPARED" );

      return Future->new->done( $response );
   });
}

=head2 $f = $conn->execute( $id, $data, $consistency )

Executes a previously-prepared statement, given its ID and the binding data.
On success, the returned Future will yield results of the same form as the
C<query> method. C<$data> should contain a list of encoded byte-string values.

Normally this method is not directly required - instead, use the C<execute>
method on the query object itself, as this will encode the parameters
correctly.

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
      $op == OPCODE_RESULT or return Future->new->fail( "Expected OPCODE_RESULT" );
      return _decode_result( $response );
   });
}

=head2 $f = $conn->register( $events )

Registers the connection's interest in receiving events of the types given in
the ARRAY reference. Event names may be C<TOPOLOGY_CHANGE>, C<STATUS_CHANGE>
or C<SCHEMA_CHANGE>. On success, the returned Future yields nothing.

=cut

sub register
{
   my $self = shift;
   my ( $events ) = @_;

   $self->send_message( OPCODE_REGISTER,
      Protocol::CassandraCQL::Frame->new->pack_string_list( $events )
   )->then( sub {
      my ( $op, $response ) = @_;
      $op == OPCODE_READY or Future->new->fail( "Expected OPCODE_READY" );

      return Future->new->done;
   });
}

=head1 TODO

=over 8

=item *

Support frame compression

=item *

Allow storing multiple Cassandra node hostnames and perform some kind of
balancing or failover of connections.

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
