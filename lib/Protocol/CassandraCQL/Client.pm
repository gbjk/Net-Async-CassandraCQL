#  You may distribute under the terms of either the GNU General Public License
#  or the Artistic License (the same terms as Perl itself)
#
#  (C) Paul Evans, 2013 -- leonerd@leonerd.org.uk

package Protocol::CassandraCQL::Client;

use strict;
use warnings;

our $VERSION = '0.03';

use base qw( IO::Socket::IP );

use Carp;

use Protocol::CassandraCQL qw( :opcodes :results );
use Protocol::CassandraCQL::Frame;
use Protocol::CassandraCQL::Result;

use constant DEFAULT_CQL_PORT => 9042;

=head1 NAME

C<Protocol::CassandraCQL::Client> - a minimal Cassandra CQL client

=head1 SYNOPSIS

 use Protocol::CassandraCQL::Client;
 use Protocol::CassandraCQL qw( CONSISTENCY_QUORUM );

 my $cass = Protocol::CassandraCQL::Client->new(
    PeerHost => "localhost",
    Keyspace => "my-keyspace",
 );

 my ( undef, $result ) = $cass->query( "SELECT v FROM numbers" );

 foreach my $row ( $result->rows_hash ) {
    say "We have a number $row->{v}";
 }

=head1 DESCRIPTION

This subclass of L<IO::Socket::IP> implements a client that can execute
queries on a Cassandra CQL database. It is not intended as a complete client,
is simply provides enough functionallity to test that the protocol handling is
working, and is used to implement the bundled F<examples/cqlsh> utility.

For a more complete client, see instead L<Net::Async::CassandraCQL>.

=cut

=head1 CONSTRUCTOR

=cut

=head2 $cass = Protocol::CassandraCQL::Client->new( %args )

Takes the following arguments in addition to those accepted by
L<IO::Socket::IP>:

=over 8

=item Keyspace => STRING

If defined, selects the keyspace to C<USE> after connection.

=back

=cut

sub new
{
   my $class = shift;
   my %args = @_ == 1 ? ( PeerHost => $_[0] ) : @_;

   $args{PeerService} ||= DEFAULT_CQL_PORT;

   my $self = $class->SUPER::new( %args ) or return;

   $self->startup;
   $self->use_keyspace( $args{Keyspace} ) if defined $args{Keyspace};

   return $self;
}

=head1 METHODS

=cut

=head2 ( $result_op, $result_frame ) = $cass->send_message( $opcode, $frame )

Sends a message with the given opcode and L<Protocol::CassandraCQL::Frame> for
the message body. Waits for a response to be received, and returns it.

If the response opcode is C<OPCODE_ERROR> then the error message string is
thrown directly as an exception; this method will only return in non-error
cases.

=cut

sub send_message
{
   my $self = shift;
   my ( $opcode, $frame ) = @_;

   $self->send( $frame->build( 0x01, 0, 0, $opcode ) );

   my ( $version, $flags, $streamid, $op, $response ) =
      Protocol::CassandraCQL::Frame->recv( $self ) or croak "Unable to ->recv: $!";

   $version == 0x81 or
      croak sprintf "Unexpected message vrsion %#02x", $version;
   # TODO: flags
   $streamid == 0 or
      croak "Unexpected stream ID $streamid";

   if( $opcode == OPCODE_ERROR ) {
      $response->unpack_int;
      croak "OPCODE_ERROR: " . $response->unpack_string;
   }

   return ( $op, $response );
}

# function
sub _decode_result
{
   my ( $response ) = @_;

   my $result = $response->unpack_int;

   if( $result == RESULT_VOID ) {
      return;
   }
   elsif( $result == RESULT_ROWS ) {
      return rows => Protocol::CassandraCQL::Result->from_frame( $response );
   }
   elsif( $result == RESULT_SET_KEYSPACE ) {
      return keyspace => $response->unpack_string;
   }
   elsif( $result == RESULT_SCHEMA_CHANGE ) {
      return schema_change => [ map { $response->unpack_string } 1 .. 3 ];
   }
   else {
      return "??" => $response->bytes;
   }
}

sub startup
{
   my $self = shift;

   my ( $op, $response ) = $self->send_message( OPCODE_STARTUP,
      Protocol::CassandraCQL::Frame->new
         ->pack_string_map( {
            CQL_VERSION => "3.0.0",
         } )
   );

   $op == OPCODE_READY or croak "Expected OPCODE_READY";
}

=head2 ( $type, $result ) = $cass->query( $cql, $consistency )

Performs a CQL query. The returned values will depend on the type of query:

For C<USE> queries, the type is C<keyspace> and C<$result> is a string giving
the name of the new keyspace.

For C<CREATE>, C<ALTER> and C<DROP> queries, the type is C<schema_change> and
C<$result> is a 3-element ARRAY reference containing the type of change, the
keyspace and the table name.

For C<SELECT> queries, the type is C<rows> and C<$result> is an instance of
L<Protocol::CassandraCQL::Result> containing the returned row data.

For other queries, such as C<INSERT>, C<UPDATE> and C<DELETE>, the method
returns nothing.

=cut

sub query
{
   my $self = shift;
   my ( $cql, $consistency ) = @_;

   my ( $op, $response ) = $self->send_message( OPCODE_QUERY,
      Protocol::CassandraCQL::Frame->new
         ->pack_lstring( $cql )
         ->pack_short( $consistency )
   );

   $op == OPCODE_RESULT or croak "Expected OPCODE_RESULT";
   return _decode_result( $response );
}

=head2 ( $type, $result ) = $cass->use_keyspace( $keyspace )

A convenient shortcut to the C<USE $keyspace> query which escapes the keyspace
name.

=cut

sub use_keyspace
{
   my $self = shift;
   my ( $keyspace ) = @_;

   # CQL's "quoting" handles any character except quote marks, which have to
   # be doubled
   $keyspace =~ s/"/""/g;

   $self->query( qq(USE "$keyspace"), 0 );
}

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
