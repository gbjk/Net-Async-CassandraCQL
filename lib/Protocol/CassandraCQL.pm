#  You may distribute under the terms of either the GNU General Public License
#  or the Artistic License (the same terms as Perl itself)
#
#  (C) Paul Evans, 2013 -- leonerd@leonerd.org.uk

package Protocol::CassandraCQL;

use strict;
use warnings;

our $VERSION = '0.01';

use Exporter 'import';
our @EXPORT_OK = qw();

=head1 NAME

C<Protocol::CassandraCQL> - wire protocol support functions for Cassandra CQLv3

=cut

# See also
#   https://github.com/apache/cassandra/blob/cassandra-1.2/doc/native_protocol.spec

my %CONSTANTS = (
   OPCODE_ERROR        => 0x00,
   OPCODE_STARTUP      => 0x01,
   OPCODE_READY        => 0x02,
   OPCODE_AUTHENTICATE => 0x03,
   OPCODE_CREDENTIALS  => 0x04,
   OPCODE_OPTIONS      => 0x05,
   OPCODE_SUPPORTED    => 0x06,
   OPCODE_QUERY        => 0x07,
   OPCODE_RESULT       => 0x08,
   OPCODE_PREPARE      => 0x09,
   OPCODE_EXECUTE      => 0x0A,
   OPCODE_REGISTER     => 0x0B,
   OPCODE_EVENT        => 0x0C,

   RESULT_VOID          => 0x0001,
   RESULT_ROWS          => 0x0002,
   RESULT_SET_KEYSPACE  => 0x0003,
   RESULT_PREPARED      => 0x0004,
   RESULT_SCHEMA_CHANGE => 0x0005,

   TYPE_CUSTOM    => 0x0000,
   TYPE_ASCII     => 0x0001,
   TYPE_BIGINT    => 0x0002,
   TYPE_BLOB      => 0x0003,
   TYPE_BOOLEAN   => 0x0004,
   TYPE_COUNTER   => 0x0005,
   TYPE_DECIMAL   => 0x0006,
   TYPE_DOUBLE    => 0x0007,
   TYPE_FLOAT     => 0x0008,
   TYPE_INT       => 0x0009,
   TYPE_TEXT      => 0x000A,
   TYPE_TIMESTAMP => 0x000B,
   TYPE_UUID      => 0x000C,
   TYPE_VARCHAR   => 0x000D,
   TYPE_VARINT    => 0x000E,
   TYPE_TIMEUUID  => 0x000F,
   TYPE_INET      => 0x0010,
   TYPE_LIST      => 0x0020,
   TYPE_MAP       => 0x0021,
   TYPE_SET       => 0x0022,

   CONSISTENCY_ANY          => 0x0000,
   CONSISTENCY_ONE          => 0x0001,
   CONSISTENCY_TWO          => 0x0002,
   CONSISTENCY_THREE        => 0x0003,
   CONSISTENCY_QUORUM       => 0x0004,
   CONSISTENCY_ALL          => 0x0005,
   CONSISTENCY_LOCAL_QUORUM => 0x0006,
   CONSISTENCY_EACH_QUORUM  => 0x0007,
);

require constant;
constant->import( $_, $CONSTANTS{$_} ) for keys %CONSTANTS;
push @EXPORT_OK, keys %CONSTANTS;

package Protocol::CassandraCQL::Frame;

# TODO: At least the lower-level methods of this class should be rewritten in
# efficient XS code

=head1 FRAME BUFFERS

=head2 $frame = Protocol::CassandraCQL::Frame->new( $bytes )

Returns a new frame buffer, optionally initialised with the given byte string.

=cut

sub new
{
   my $class = shift;
   my $bytes = $_[0] // "";
   bless \$bytes, $class;
}

=head2 $bytes = $frame->bytes

Returns the byte string currently in the buffer.

=cut

sub bytes { ${$_[0]} }

=head2 $frame->pack_short( $v )

=head2 $v = $frame->unpack_short

Add or remove a short value.

=cut

sub pack_short { ${$_[0]} .= pack "S>", $_[1];
                 $_[0] }
sub unpack_short { unpack "S>", substr ${$_[0]}, 0, 2, "" }

=head2 $frame->pack_int( $v )

=head2 $v = $frame->unpack_int

Add or remove an int value.

=cut

sub pack_int { ${$_[0]} .= pack "l>", $_[1];
               $_[0] }
sub unpack_int { unpack "l>", substr ${$_[0]}, 0, 4, "" }

=head2 $frame->pack_string( $v )

=head2 $v = $frame->unpack_string

Add or remove a string value.

=cut

# TODO: UTF-8 encoding
sub pack_string { $_[0]->pack_short( length $_[1] );
                  ${$_[0]} .= $_[1];
                  $_[0] }
sub unpack_string { my $l = $_[0]->unpack_short;
                    substr ${$_[0]}, 0, $l, "" }

=head2 $frame->pack_lstring( $v )

=head2 $v = $frame->unpack_lstring

Add or remove a long string value.

=cut

# TODO: UTF-8 encoding
sub pack_lstring { $_[0]->pack_int( length $_[1] );
                   ${$_[0]} .= $_[1];
                   $_[0] }
sub unpack_lstring { my $l = $_[0]->unpack_int;
                     substr ${$_[0]}, 0, $l, "" }

=head2 $frame->pack_string_list( $v )

=head2 $v = $frame->unpack_string_list

Add or remove a list of strings from or to an ARRAYref

=cut

sub pack_string_list { $_[0]->pack_short( scalar @{$_[1]} );
                       $_[0]->pack_string($_) for @{$_[1]};
                       $_[0] }
sub unpack_string_list { my $n = $_[0]->unpack_short;
                         [ map { $_[0]->unpack_string } 1 .. $n ] }

=head2 $frame->pack_bytes( $v )

=head2 $v = $frame->unpack_bytes

Add or remove opaque bytes or C<undef>.

=cut

sub pack_bytes { if( defined $_[1] ) { $_[0]->pack_int( length $_[1] ); ${$_[0]} .= $_[1] }
                 else                { $_[0]->pack_int( -1 ) }
                 $_[0] }
sub unpack_bytes { my $l = $_[0]->unpack_int;
                   $l > 0 ? substr ${$_[0]}, 0, $l, "" : undef }

=head2 $frame->pack_short_bytes( $v )

=head2 $v = $frame->unpack_short_bytes

Add or remove opaque short bytes.

=cut

sub pack_short_bytes { $_[0]->pack_short( length $_[1] );
                       ${$_[0]} .= $_[1];
                       $_[0] }
sub unpack_short_bytes { my $l = $_[0]->unpack_short;
                         substr ${$_[0]}, 0, $l, "" }

=head2 $frame->pack_string_map( $v )

=head2 $v = $frame->unpack_string_map

Add or remove a string map from or to a HASH of strings.

=cut

# Don't strictly need to sort the keys but it's nice for unit testing
sub pack_string_map { $_[0]->pack_short( scalar keys %{$_[1]} );
                      $_[0]->pack_string( $_ ), $_[0]->pack_string( $_[1]->{$_} ) for sort keys %{$_[1]};
                      $_[0] }
sub unpack_string_map { my $n = $_[0]->unpack_short;
                        +{ map { $_[0]->unpack_string => $_[0]->unpack_string } 1 .. $n } }

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
