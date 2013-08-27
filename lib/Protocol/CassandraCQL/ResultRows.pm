#  You may distribute under the terms of either the GNU General Public License
#  or the Artistic License (the same terms as Perl itself)
#
#  (C) Paul Evans, 2013 -- leonerd@leonerd.org.uk

package Protocol::CassandraCQL::ResultRows;

use strict;
use warnings;

our $VERSION = '0.01';

use Protocol::CassandraCQL qw( :types );

=head1 NAME

C<Protocol::CassandraCQL::ResultRows> - stores the result of a Cassandra CQL query

=cut

=head1 CONSTRUCTOR

=head2 $result = Protocol::CassandraCQL::ResultRows->new( $frame )

Returns a new result object initialised from the given C<OPCODE_RESULT> /
C<RESULT_ROWS> message frame.

=cut

sub new
{
   my $class = shift;
   my ( $frame ) = @_;

   my $self = bless {}, $class;

   $self->_unpack_metadata( $frame );

   my $n_rows = $frame->unpack_int;
   my $n_columns = scalar @{$self->{columns}};
   foreach ( 1 .. $n_rows ) {
      push @{$self->{rows}}, [ map { $frame->unpack_bytes } 1 .. $n_columns ];
   }

   return $self;
}

sub _unpack_metadata
{
   my $self = shift;
   my ( $frame ) = @_;

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
}

=head1 METHODS

=cut

=head2 $n = $result->columns

Returns the number of columns

=cut

sub columns
{
   my $self = shift;
   return scalar @{ $self->{columns} };
}

=head2 $name = $result->column_name( $idx )

=head2 ( $keyspace, $table, $column ) = $result->column_name( $idx )

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

=head2 $n = $result->rows

Returns the number of rows

=cut

sub rows
{
   my $self = shift;
   return scalar @{ $self->{rows} };
}

=head2 @columns = $result->rowbytes( $idx )

Returns a list of the raw byte blobs containing the row's data

=cut

sub rowbytes
{
   my $self = shift;
   my ( $idx ) = @_;

   return @{ $self->{rows}[$idx] };
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
