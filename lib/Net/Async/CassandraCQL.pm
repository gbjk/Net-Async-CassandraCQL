#  You may distribute under the terms of either the GNU General Public License
#  or the Artistic License (the same terms as Perl itself)
#
#  (C) Paul Evans, 2013 -- leonerd@leonerd.org.uk

package Net::Async::CassandraCQL;

use strict;
use warnings;
use 5.010;

our $VERSION = '0.05';

use base qw( IO::Async::Notifier );

use Carp;

use Protocol::CassandraCQL qw( CONSISTENCY_ONE );

use Net::Async::CassandraCQL::Query;

=head1 NAME

C<Net::Async::CassandraCQL> - use Cassandra databases with L<IO::Async> using CQL

=head1 SYNOPSIS

 use IO::Async::Loop;
 use Net::Async::CassandraCQL;
 use Protocol::CassandraCQL qw( CONSISTENCY_QUORUM );

 my $loop = IO::Async::Loop->new;

 my $cass = Net::Async::CassandraCQL->new(
    host => "localhost",
    keyspace => "my-keyspace",
    default_consistency => CONSISTENCY_QUORUM,
 );
 $loop->add( $cass );


 $cass->connect->get;


 my @f;
 foreach my $number ( 1 .. 100 ) {
    push @f, $cass->query( "INSERT INTO numbers (v) VALUES $number" );
 }
 Future->needs_all( @f )->get;


 my $get_stmt = $cass->prepare( "SELECT v FROM numbers" )->get;

 my ( undef, $result ) = $get_stmt->execute( [] )->get;

 foreach my $row ( $result->rows_hash ) {
    say "We have a number " . $row->{v};
 }

=head1 DESCRIPTION

This module allows use of the C<CQL3> interface of a Cassandra database. It
fully supports asynchronous operation via L<IO::Async>, allowing both direct
queries and prepared statements to be managed concurrently, if required.
Alternatively, as the interface is entirely based on L<Future> objects, it can
be operated synchronously in a blocking fashion by simply awaiting each
individual operation by calling the C<get> method.

It is based on L<Protocol::CassandraCQL>, which more completely documents the
behaviours and limits of its ability to communicate with Cassandra.

=cut

=head1 PARAMETERS

The following named parameters may be passed to C<new> or C<configure>:

=over 8

=item host => STRING

The hostname of the Cassandra node to connect to

=item service => STRING

Optional. The service name or port number to connect to.

=item username => STRING

=item password => STRING

Optional. Authentication details to use for C<PasswordAuthenticator>.

=item keyspace => STRING

Optional. If set, a C<USE keyspace> query will be issued as part of the
connect method.

=item default_consistency => INT

Optional. Default consistency level to use if none is provided to C<query> or
C<execute>.

=back

=cut

sub configure
{
   my $self = shift;
   my %params = @_;

   foreach (qw( host service username password keyspace default_consistency )) {
      $self->{$_} = delete $params{$_} if exists $params{$_};
   }

   $self->SUPER::configure( %params );
}

=head1 METHODS

=cut

=head2 $str = $cass->quote( $str )

Quotes a string argument suitable for inclusion in an immediate CQL query
string.

In general, it is better to use a prepared query and pass the value as an
execute parameter though.

=cut

sub quote
{
   my $self = shift;
   my ( $str ) = @_;

   # CQL's 'quoting' handles any character except quote marks, which have to
   # be doubled
   $str =~ s/'/''/g;
   return qq('$str');
}

=head2 $str = $cass->quote_identifier( $str )

Quotes an identifier name suitable for inclusion in a CQL query string.

=cut

sub quote_identifier
{
   my $self = shift;
   my ( $str ) = @_;

   return $str if $str =~ m/^[a-z_][a-z0-9_]+$/;

   # CQL's "quoting" handles any character except quote marks, which have to
   # be doubled
   $str =~ s/"/""/g;
   return qq("$str");
}

=head2 $f = $cass->connect( %args )

Connects to the Cassandra node and starts up the connection. The returned
Future will yield nothing on success.

Takes the following named arguments:

=over 8

=item host => STRING

=item service => STRING

=item keyspace => STRING

Optional. Overrides the configured values.

=back

A host name is required, either as a named argument or as a configured value
on the object. If the service name is missing, the default CQL port will be
used instead.

=cut

sub connect
{
   my $self = shift;
   my %args = @_;

   # Must be late
   require Net::Async::CassandraCQL::Connection;

   ( $self->{conn} ||= do {
         my $conn = Net::Async::CassandraCQL::Connection->new(
            map { $_ => $self->{$_} } qw( host service username password keyspace )
         );
         $self->add_child( $conn );
         $conn;
   } )->connect( %args );
}

=head2 $f = $cass->query( $cql, $consistency )

Performs a CQL query. On success, the values returned from the Future will
depend on the type of query.

 ( $type, $result ) = $f->get

For C<USE> queries, the type is C<keyspace> and C<$result> is a string giving
the name of the new keyspace.

For C<CREATE>, C<ALTER> and C<DROP> queries, the type is C<schema_change> and
C<$result> is a 3-element ARRAY reference containing the type of change, the
keyspace and the table name.

For C<SELECT> queries, the type is C<rows> and C<$result> is an instance of
L<Protocol::CassandraCQL::Result> containing the returned row data.

For other queries, such as C<INSERT>, C<UPDATE> and C<DELETE>, the future
returns nothing.

=cut

sub query
{
   my $self = shift;
   my ( $cql, $consistency ) = @_;

   $consistency //= $self->{default_consistency};
   defined $consistency or croak "'query' needs a consistency level";

   $self->{conn}->query( $cql, $consistency );
}

=head2 $f = $cass->query_rows( $cql, $consistency )

A shortcut wrapper for C<query> which expects a C<rows> result and returns it
directly. Any other result is treated as an error. The returned Future returns
a C<Protocol::CassandraCQL::Result> directly

 $result = $f->get

=cut

sub query_rows
{
   my $self = shift;
   my ( $cql, $consistency ) = @_;

   $self->query( $cql, $consistency )->then( sub {
      my ( $type, $result ) = @_;
      $type eq "rows" or Future->new->fail( "Expected 'rows' result" );
      Future->new->done( $result );
   });
}

=head2 $f = $cass->prepare( $cql )

Prepares a CQL query for later execution. On success, the returned Future
yields an instance of a prepared query object (see below).

 ( $query ) = $f->get

=cut

sub prepare
{
   my $self = shift;
   my ( $cql ) = @_;

   $self->{conn}->prepare( $cql )->then( sub {
      my ( $frame ) = @_;

      return Future->new->done( Net::Async::CassandraCQL::Query->from_frame( $self, $frame ) );
   } );
}

=head2 $f = $cass->execute( $id, $data, $consistency )

Executes a previously-prepared statement, given its ID and the binding data.
On success, the returned Future will yield results of the same form as the
C<query> method. C<$data> should contain a list of encoded byte-string values.

Normally this method is not directly required - instead, use the C<execute>
method on the query object itself, as this will encode the parameters
correctly.

=cut

sub execute
{
   my $self = shift;
   my ( $id, $data, $consistency ) = @_;

   $consistency //= $self->{default_consistency};
   defined $consistency or croak "'execute' needs a consistency level";

   $self->{conn}->execute( $id, $data, $consistency );
}

=head1 CONVENIENT WRAPPERS

The following wrapper methods all wrap the basic C<query> operation.

=cut

=head2 $f = $cass->schema_keyspaces

A shortcut to a C<SELECT> query on C<system.schema_keyspaces>, which returns a
result object listing all the keyspaces.

 ( $result ) = $f->get

Exact details of the returned columns will depend on the Cassandra version,
but the result should at least be keyed by the first column, called
C<keyspace_name>.

 my $keyspaces = $result->rowmap_hash( "keyspace_name" )

=cut

sub schema_keyspaces
{
   my $self = shift;

   $self->query_rows(
      "SELECT * FROM system.schema_keyspaces",
      CONSISTENCY_ONE
   );
}

=head2 $f = $cass->schema_columnfamilies( $keyspace )

A shortcut to a C<SELECT> query on C<system.schema_columnfamilies>, which
returns a result object listing all the columnfamilies of the given keyspace.

 ( $result ) = $f->get

Exact details of the returned columns will depend on the Cassandra version,
but the result should at least be keyed by the first column, called
C<columnfamily_name>.

 my $columnfamilies = $result->rowmap_hash( "columnfamily_name" )

=cut

sub schema_columnfamilies
{
   my $self = shift;
   my ( $keyspace ) = @_;

   $self->query_rows(
      "SELECT * FROM system.schema_columnfamilies WHERE keyspace_name = " . $self->quote( $keyspace ),
      CONSISTENCY_ONE
   );
}

=head2 $f = $cass->schema_columns( $keyspace, $columnfamily )

A shortcut to a C<SELECT> query on C<system.schema_columns>, which returns a
result object listing all the columns of the given columnfamily.

 ( $result ) = $f->get

Exact details of the returned columns will depend on the Cassandra version,
but the result should at least be keyed by the first column, called
C<column_name>.

 my $columns = $result->rowmap_hash( "column_name" )

=cut

sub schema_columns
{
   my $self = shift;
   my ( $keyspace, $columnfamily ) = @_;

   $self->query_rows(
      "SELECT * FROM system.schema_columns WHERE keyspace_name = " . $self->quote( $keyspace ) . " AND columnfamily_name = " . $self->quote( $columnfamily ),
      CONSISTENCY_ONE,
   );
}

=head2 $f = $cass->local_info

A shortcut to a C<SELECT> query on C<system.local> and returning the (only)
row in the result as a HASH reference.

 ( $local ) = $f->get

=cut

sub local_info
{
   my $self = shift;

   $self->query_rows(
      "SELECT * FROM system.local",
      CONSISTENCY_ONE,
   )->then( sub {
      my ( $result ) = @_;
      return Future->new->done( $result->row_hash( 0 ) )
   });
}

=head2 $f = $cass->peers_info

A shortcut to a C<SELECT> query on C<system.peers> and returning the rows in
the result as a rowmap keyed by <TODO>.

 $peermap = $f->get

=cut

sub peers_info
{
   my $self = shift;

   $self->query_rows(
      "SELECT * FROM system.peers",
      CONSISTENCY_ONE,
   )->then( sub {
      my ( $result ) = @_;
      return Future->new->done( [ $result->rows_hash ] )
   });
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
