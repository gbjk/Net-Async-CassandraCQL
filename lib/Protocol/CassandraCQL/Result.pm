#  You may distribute under the terms of either the GNU General Public License
#  or the Artistic License (the same terms as Perl itself)
#
#  (C) Paul Evans, 2013 -- leonerd@leonerd.org.uk

package Protocol::CassandraCQL::Result;

use strict;
use warnings;
use base qw( Protocol::CassandraCQL::ColumnMeta );

our $VERSION = '0.01';

use Protocol::CassandraCQL qw( :types );

=head1 NAME

C<Protocol::CassandraCQL::Result> - stores the result of a Cassandra CQL query

=head1 DESCRIPTION

This is a subclass of L<Protocol::CassandraCQL::ColumnMeta>.

=cut

=head1 CONSTRUCTOR

=head2 $result = Protocol::CassandraCQL::Result->new( $frame )

Returns a new result object initialised from the given C<OPCODE_RESULT> /
C<RESULT_ROWS> message frame.

=cut

sub new
{
   my $class = shift;
   my ( $frame ) = @_;
   my $self = $class->SUPER::new( $frame );

   my $n_rows = $frame->unpack_int;
   my $n_columns = scalar @{$self->{columns}};

   $self->{rows} = [];
   foreach ( 1 .. $n_rows ) {
      push @{$self->{rows}}, [ map { $frame->unpack_bytes } 1 .. $n_columns ];
   }

   return $self;
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

=head1 TODO

=over 8

=item *

Decode column values from byte buffers according to their type. This will
require some research as the basic protocol doc doesn't explain how that
works.

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
