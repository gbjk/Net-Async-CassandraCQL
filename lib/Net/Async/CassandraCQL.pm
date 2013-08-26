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

use Protocol::CassandraCQL qw(
   OPCODE_ERROR
);

use constant DEFAULT_CQL_PORT => 9042;

=head1 NAME

C<Net::Async::CassandraCQL> - use Cassandra databases with L<IO::Async> using CQL

=cut

=head1 METHODS

=cut

sub connect
{
   my $self = shift;
   my %args = @_;

   $args{service} //= DEFAULT_CQL_PORT;

   return $self->{connect_f} ||=
      $self->SUPER::connect( %args )->on_fail( sub { undef $self->{connect_f} } );
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

   # TODO: flags
   if( my $f = $self->{streams}[$streamid] ) {
      if( $opcode == OPCODE_ERROR ) {
         my ( $err, $message ) = unpack "N n/a*", $body;
         $f->fail( "OPCODE_ERROR: $message\n", $body, $err );
      }
      else {
         $f->done( $opcode, $body );
      }

      undef $self->{streams}[$streamid];
   }
   else {
      print STDERR "Received a message opcode=$opcode for unknown stream $streamid\n";
   }

   return 1;
}

=head2 $f = $cass->send_message( $opcode, $body )

Sends a message with the given opcode and body. The returned Future will yield
the response opcode and body.

  ( $reply_opcode, $reply_body ) = $f->get

=cut

sub send_message
{
   my $self = shift;
   my ( $opcode, $body ) = @_;

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
   $self->write( pack "C C C C N a*", $version, $flags, $id, $opcode, length $body, $body );

   return $streams->[$id] = $self->loop->new_future;
}

=head1 TODO

=over 8

=item *

Queue messages if all 127 available stream IDs are already consumed.

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
