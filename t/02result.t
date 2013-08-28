#!/usr/bin/perl

use strict;
use warnings;

use Test::More;

use Protocol::CassandraCQL::Frame;
use Protocol::CassandraCQL::Result;

# Single column/row
{
   my $result = Protocol::CassandraCQL::Result->new(
      Protocol::CassandraCQL::Frame->new(
         "\0\0\0\1\0\0\0\1\0\4test\0\5table\0\6column\0\x0a" . # metadata
         "\0\0\0\1" .   # row count
         "\0\0\0\4data" # row 0
      )
   );

   is( scalar $result->columns, 1, '$result->columns is 1' );
   is_deeply( [ $result->column_name( 0 ) ],
              [qw( test table column )],
              '$result->column_name(0) list' );
   is( scalar $result->column_name( 0 ),
       "test.table.column",
       '$result->column_name(0) scalar' );

   is( $result->column_type(0), "TEXT", '$result->column_type(0)' );

   is( scalar $result->rows, 1, '$result->rows is 1' );

   is_deeply( [ $result->rowbytes( 0 ) ],
              [ "data" ],
              '$result->rowbytes(0)' );
}

done_testing;
