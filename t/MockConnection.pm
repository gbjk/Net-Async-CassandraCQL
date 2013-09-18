package t::MockConnection;

use strict;
use warnings;
use base qw( Net::Async::CassandraCQL::Connection );

sub new
{
   my $class = shift;
   my ( $nodeid ) = @_;
   my $self = $class->SUPER::new;

   $self->{nodeid} = $nodeid;
   $self->{pending_queries} = [];
   $self->{pending_prepares} = [];

   return $self;
}

# Mocking API
sub next_query
{
   my $self = shift;
   return shift @{$self->{pending_queries}};
}

sub next_prepare
{
   my $self = shift;
   return shift @{$self->{pending_prepares}};
}

sub is_registered
{
   my $self = shift;
   return $self->{is_registered};
}

sub query
{
   my $self = shift;
   my ( $cql ) = @_;
   push @{$self->{pending_queries}}, [ $self->nodeid, $cql, my $f = Future->new ];
   return $f;
}

sub prepare
{
   my $self = shift;
   my ( $cql ) = @_;
   push @{$self->{pending_prepares}}, [ $self->nodeid, $cql, my $f = Future->new ];
   return $f;
}

sub register
{
   my $self = shift;
   $self->{is_registered} = 1;
   return Future->new->done;
}

0x55AA;
