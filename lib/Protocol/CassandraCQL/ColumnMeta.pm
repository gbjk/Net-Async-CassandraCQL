#  You may distribute under the terms of either the GNU General Public License
#  or the Artistic License (the same terms as Perl itself)
#
#  (C) Paul Evans, 2013 -- leonerd@leonerd.org.uk

package Protocol::CassandraCQL::ColumnMeta;

use strict;
use warnings;

our $VERSION = '0.01';

use Protocol::CassandraCQL qw( :types );

=head1 NAME

C<Protocol::CassandraCQL::ColumnMeta> - stores the column metadata result of a Cassandra CQL query

=cut

=head1 CONSTRUCTOR

=head2 $meta = Protocol::CassandraCQL::ColumnMeta->new( $frame )

Returns a new result object initialised from the given C<OPCODE_RESULT> /
C<RESULT_ROWS> or C<RESULT_PREPARED> message frame.

=cut

sub new
{
   my $class = shift;
   my ( $frame ) = @_;

   my $self = bless {}, $class;

   my $flags     = $frame->unpack_int;
   my $n_columns = $frame->unpack_int;

   my $has_gts = $flags & 0x0001;
   my @gts = $has_gts ? ( $frame->unpack_string, $frame->unpack_string )
                      : ();

   foreach ( 1 .. $n_columns ) {
      my @keyspace_table = $has_gts ? @gts : ( $frame->unpack_string, $frame->unpack_string );
      my $colname        = $frame->unpack_string;

      my $typeid = $frame->unpack_short;
      my @col = ( @keyspace_table, $colname, $typeid );

      if( $typeid == TYPE_CUSTOM ) {
         push @col, $frame->unpack_string;
      }

      push @{$self->{columns}}, \@col;
   }

   return $self;
}

=head1 METHODS

=cut

=head2 $n = $meta->columns

Returns the number of columns

=cut

sub columns
{
   my $self = shift;
   return scalar @{ $self->{columns} };
}

=head2 $name = $meta->column_name( $idx )

=head2 ( $keyspace, $table, $column ) = $meta->column_name( $idx )

Returns the name of the column at the given (0-based) index; either as three
separate strings, or all joined by ".".

=cut

sub column_name
{
   my $self = shift;
   my ( $idx ) = @_;

   my @n = @{ $self->{columns}[$idx] }[0..2];

   return @n if wantarray;
   return join ".", @n;
}

=head2 $type = $meta->column_type( $idx )

Returns the type name of the column at the given index.

=cut

sub column_type
{
   my $self = shift;
   my ( $idx ) = @_;

   my ( $typeid, $custom ) = @{ $self->{columns}[$idx] }[3,4];
   return $custom if $typeid == TYPE_CUSTOM;

   return Protocol::CassandraCQL::typename( $typeid );
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
