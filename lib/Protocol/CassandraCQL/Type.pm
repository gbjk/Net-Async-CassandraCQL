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
use Scalar::Util qw( blessed );

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
   if( $typeid == TYPE_CUSTOM ) {
      return Protocol::CassandraCQL::Type::Custom->from_frame( $frame );
   }
   else {
      return Protocol::CassandraCQL::Type->new( $typeid );
   }
}

# Just for unit testing
sub from_name
{
   shift;
   my ( $name ) = @_;

   defined( my $typeid = Protocol::CassandraCQL->can( "TYPE_$name" )->() ) or
      croak "No such type '$name'";
   return Protocol::CassandraCQL::Type->new( $typeid );
}

sub new
{
   my $class = shift;
   bless [ $_[0] ], $class;
}

=head1 METHODS

=cut

=head2 $name = $type->name

Returns a string representation of the type name.

=cut

sub name
{
   my $self = shift;
   return Protocol::CassandraCQL::typename( $self->id );
}

=head2 $id = $type->id

Returns the basic type ID as the value of a C<TYPE_*> constant.

=cut

sub id
{
   my $self = shift;
   return $self->[0];
}

# TODO: bind encode/decode methods at instantiation
#  Or better yet, subclass

=head2 $bytes = $type->encode( $v )

Encodes the given perl data into a bytestring.

=cut

sub encode
{
   my $self = shift;
   my ( $v ) = @_;

   my $type = $self->name;
   if( my $code = __PACKAGE__->can( "encode_$type" ) ) {
      return $code->( $v );
   }
   else {
      warn "Not sure how to encode $type";
      return $v;
   }
}

=head2 $v = $type->decode( $bytes )

Decodes the given bytestring into perl data.

=cut

sub decode
{
   my $self = shift;
   my ( $b ) = @_;

   my $type = $self->name;
   if( my $code = __PACKAGE__->can( "decode_$type" ) ) {
      return $code->( $b );
   }
   else {
      warn "Not sure how to decode $type";
      # Fallback to a text-safe hexbytes representation
      return unpack "H*", $b;
   }
}

#      if( $typeid == TYPE_CUSTOM ) {
#         push @col, $frame->unpack_string;
#      }

#   my ( $typeid, $custom ) = @{ $self->{columns}[$idx] }[4,5];
#   return $custom if $typeid == TYPE_CUSTOM;

# Now the codecs

# ASCII-only bytes
sub encode_ASCII { $_[0] =~ m/^[\x00-\x7f]*$/ or die "Non-ASCII"; $_[0] }
sub decode_ASCII { $_[0] }

# 64-bit integer
sub encode_BIGINT { pack   "q>", $_[0] }
sub decode_BIGINT { unpack "q>", $_[0] }

# blob
sub encode_BLOB { $_[0] }
sub decode_BLOB { $_[0] }

# true/false byte
sub encode_BOOLEAN { pack   "C", !!$_[0] }
sub decode_BOOLEAN { !!unpack "C", $_[0] }

# counter is a 64-bit integer
*encode_COUNTER = \&encode_BIGINT;
*decode_COUNTER = \&decode_BIGINT;

# Not clearly docmuented, but this appears to be an INT decimal shift followed
# by a VARINT
sub encode_DECIMAL {
   require Math::BigFloat;
   my $shift = $_[0] =~ m/\.(\d*)$/ ? length $1 : 0;
   my $n = blessed $_[0] ? $_[0] : Math::BigFloat->new( $_[0] );
   return pack( "L>", $shift ) . encode_VARINT( $n->blsft($shift, 10) );
}

sub decode_DECIMAL {
   require Math::BigFloat;
   my $shift = unpack "L>", $_[0];
   my $n = decode_VARINT( substr $_[0], 4 );
   return scalar Math::BigFloat->new($n)->brsft($shift, 10);
}

# IEEE double
sub encode_DOUBLE { pack   "d>", $_[0] }
sub decode_DOUBLE { unpack "d>", $_[0] }

# IEEE single
sub encode_FLOAT { pack   "f>", $_[0] }
sub decode_FLOAT { unpack "f>", $_[0] }

# 32-bit integer
sub encode_INT { pack   "l>", $_[0] }
sub decode_INT { unpack "l>", $_[0] }

# 'text' seems to come back as 'varchar' but we'll leave them both aliased
*encode_VARCHAR = *encode_TEXT = \&Encode::encode_utf8;
*decode_VARCHAR = *decode_TEXT = \&Encode::decode_utf8;

# miliseconds since UNIX epoch as 64bit uint
sub encode_TIMESTAMP {  pack   "Q>", ($_[0] * 1000) }
sub decode_TIMESTAMP { (unpack "Q>", $_[0]) / 1000  }

# TODO: UUID

# Arbitrary-precision 2s-complement signed integer
# Math::BigInt doesn't handle signed, but we can mangle it
sub encode_VARINT {
   require Math::BigInt;
   my $n = blessed $_[0] ? $_[0] : Math::BigInt->new($_[0]); # upgrade to a BigInt

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

sub decode_VARINT {
   require Math::BigInt;

   if( unpack( "C", $_[0] ) >= 0x80 ) {
      return -Math::BigInt->from_hex( unpack "H*", ~$_[0] ) - 1;
   }
   else {
      return Math::BigInt->from_hex( unpack "H*", $_[0] );
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
