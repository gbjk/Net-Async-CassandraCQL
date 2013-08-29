#  You may distribute under the terms of either the GNU General Public License
#  or the Artistic License (the same terms as Perl itself)
#
#  (C) Paul Evans, 2013 -- leonerd@leonerd.org.uk

package Protocol::CassandraCQL::Type;

use strict;
use warnings;

our $VERSION = '0.01';

use Carp;

use Encode ();

use Protocol::CassandraCQL qw( :types );

=head1 NAME

C<Protocol::CassandraCQL::Type> - represents a Cassandra CQL data type

=head1 DESCRIPTION

Objects in this class represent distinct types that may be found in Cassandra
CQLv3, either as columns in query result rows, or as bind parameters to
prepared statements. It is used by L<Protocol::CassandraCQL::ColumnMeta>.

=cut

=head1 CONSTRUCTOR

=head2 $type = Protocol::CassandraCQL::Type->from_frame( $frame )

Returns a new type object initialised by parsing the type information in the
given message frame.

=cut

sub from_frame
{
   shift; # ignore
   my ( $frame ) = @_;

   my $typeid = $frame->unpack_short;
   return Protocol::CassandraCQL::Type->from_name( Protocol::CassandraCQL::typename( $typeid ) );
}

# Just for unit testing
sub from_name
{
   shift;
   my ( $name ) = @_;

   my $class = "Protocol::CassandraCQL::Type::$name";
   die "Unrecognised type name '$name'" unless $class->can( "new" );
   $class->new
}

sub new
{
   my $class = shift;
   return bless [], $class;
}

=head1 METHODS

=cut

=head2 $name = $type->name

Returns a string representation of the type name.

=cut

sub name
{
   my $self = shift;
   return +( ( ref $self ) =~ m/::([^:]+)$/ )[0];
}

=head2 $bytes = $type->encode( $v )

Encodes the given perl data into a bytestring.

=head2 $v = $type->decode( $bytes )

Decodes the given bytestring into perl data.

=cut

#      if( $typeid == TYPE_CUSTOM ) {
#         push @col, $frame->unpack_string;
#      }

#   my ( $typeid, $custom ) = @{ $self->{columns}[$idx] }[4,5];
#   return $custom if $typeid == TYPE_CUSTOM;

# Now the codecs

# ASCII-only bytes
package Protocol::CassandraCQL::Type::ASCII;
use base qw( Protocol::CassandraCQL::Type );
sub encode { $_[1] =~ m/^[\x00-\x7f]*$/ or die "Non-ASCII"; $_[1] }
sub decode { $_[1] }

# 64-bit integer
package Protocol::CassandraCQL::Type::BIGINT;
use base qw( Protocol::CassandraCQL::Type );
sub encode { pack   "q>", $_[1] }
sub decode { unpack "q>", $_[1] }

# blob
package Protocol::CassandraCQL::Type::BLOB;
use base qw( Protocol::CassandraCQL::Type );
sub encode { $_[1] }
sub decode { $_[1] }

# true/false byte
package Protocol::CassandraCQL::Type::BOOLEAN;
use base qw( Protocol::CassandraCQL::Type );
sub encode { pack   "C", !!$_[1] }
sub decode { !!unpack "C", $_[1] }

# counter is a 64-bit integer
package Protocol::CassandraCQL::Type::COUNTER;
use base qw( Protocol::CassandraCQL::Type::BIGINT );

# Not clearly docmuented, but this appears to be an INT decimal shift followed
# by a VARINT
package Protocol::CassandraCQL::Type::DECIMAL;
use base qw( Protocol::CassandraCQL::Type );
use Scalar::Util qw( blessed );
sub encode {
   require Math::BigFloat;
   my $shift = $_[1] =~ m/\.(\d*)$/ ? length $1 : 0;
   my $n = blessed $_[1] ? $_[1] : Math::BigFloat->new( $_[1] );
   return pack( "L>", $shift ) . Protocol::CassandraCQL::Type::VARINT->encode( $n->blsft($shift, 10) );
}
sub decode {
   require Math::BigFloat;
   my $shift = unpack "L>", $_[1];
   my $n = Protocol::CassandraCQL::Type::VARINT->decode( substr $_[1], 4 );
   return scalar Math::BigFloat->new($n)->brsft($shift, 10);
}

# IEEE double
package Protocol::CassandraCQL::Type::DOUBLE;
use base qw( Protocol::CassandraCQL::Type );
sub encode { pack   "d>", $_[1] }
sub decode { unpack "d>", $_[1] }

# IEEE single
package Protocol::CassandraCQL::Type::FLOAT;
use base qw( Protocol::CassandraCQL::Type );
sub encode { pack   "f>", $_[1] }
sub decode { unpack "f>", $_[1] }

# 32-bit integer
package Protocol::CassandraCQL::Type::INT;
use base qw( Protocol::CassandraCQL::Type );
sub encode { pack   "l>", $_[1] }
sub decode { unpack "l>", $_[1] }

# UTF-8 text
package Protocol::CassandraCQL::Type::VARCHAR;
use base qw( Protocol::CassandraCQL::Type );
sub encode { Encode::encode_utf8 $_[1] }
sub decode { Encode::decode_utf8 $_[1] }

# 'text' seems to come back as 'varchar'
package Protocol::CassandraCQL::Type::TEXT;
use base qw( Protocol::CassandraCQL::Type::VARCHAR );

# miliseconds since UNIX epoch as 64bit uint
package Protocol::CassandraCQL::Type::TIMESTAMP;
use base qw( Protocol::CassandraCQL::Type );
sub encode {  pack   "Q>", ($_[1] * 1000) }
sub decode { (unpack "Q>", $_[1]) / 1000  }

# TODO: UUID

# Arbitrary-precision 2s-complement signed integer
# Math::BigInt doesn't handle signed, but we can mangle it
package Protocol::CassandraCQL::Type::VARINT;
use base qw( Protocol::CassandraCQL::Type );
use Scalar::Util qw( blessed );
sub encode {
   require Math::BigInt;
   my $n = blessed $_[1] ? $_[1] : Math::BigInt->new($_[1]); # upgrade to a BigInt

   my $bytes;
   if( $n < 0 ) {
      my $hex = substr +(-$n-1)->as_hex, 2;
      $hex = "0$hex" if length($hex) % 2;
      $bytes = ~(pack "H*", $hex);
      # Sign-extend if required to avoid appearing positive
      $bytes = "\xff$bytes" if unpack( "C", $bytes ) < 0x80;
   }
   else {
      my $hex = substr $n->as_hex, 2; # trim 0x
      $hex = "0$hex" if length($hex) % 2;
      $bytes = pack "H*", $hex;
      # Zero-extend if required to avoid appearing negative
      $bytes = "\0$bytes" if unpack( "C", $bytes ) >= 0x80;
   }
   $bytes;
}
sub decode {
   require Math::BigInt;

   if( unpack( "C", $_[1] ) >= 0x80 ) {
      return -Math::BigInt->from_hex( unpack "H*", ~$_[1] ) - 1;
   }
   else {
      return Math::BigInt->from_hex( unpack "H*", $_[1] );
   }
}

# TODO: INET

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