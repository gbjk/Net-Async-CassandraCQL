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
);

require constant;
constant->import( $_, $CONSTANTS{$_} ) for keys %CONSTANTS;
push @EXPORT_OK, keys %CONSTANTS;

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
