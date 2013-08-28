#!/usr/bin/perl

use strict;
use warnings;

use Test::More;
use Test::HexString;

use Protocol::CassandraCQL::Frame;
use Protocol::CassandraCQL::ColumnMeta;

{
   my $meta = Protocol::CassandraCQL::ColumnMeta->new(
      Protocol::CassandraCQL::Frame->new(
         "\0\0\0\1\0\0\0\3\0\4test\0\5table\0\3key\0\x0a\0\1i\0\x09\0\1b\0\x02"
      )
   );

   is( scalar $meta->columns, 3, '$meta->columns is 3' );

   is_deeply( [ $meta->column_name( 0 ) ],
              [qw( test table key )],
              '$meta->column_name(0) list' );
   is( scalar $meta->column_name( 0 ),
       "test.table.key",
       '$meta->column_name(0) scalar' );
   is_deeply( [ $meta->column_name( 1 ) ],
              [qw( test table i )],
              '$meta->column_name(1) list' );
   is_deeply( [ $meta->column_name( 2 ) ],
              [qw( test table b )],
              '$meta->column_name(2) list' );

   is( $meta->column_type(0), "TEXT",   '$meta->column_type(0)' );
   is( $meta->column_type(1), "INT",    '$meta->column_type(1)' );
   is( $meta->column_type(2), "BIGINT", '$meta->column_type(2)' );

   my @bytes = $meta->encode_data( "the-key", 123, 456 );
   is_hexstr( $bytes[0], "the-key",              '->encode_data [0]' );
   is_hexstr( $bytes[1], "\0\0\0\x7b",           '->encode_data [1]' );
   is_hexstr( $bytes[2], "\0\0\0\0\0\0\x01\xc8", '->encode_data [2]' );

   is_deeply( [ $meta->decode_data( "another-key", "\0\0\0\x7c", "\0\0\0\0\0\0\x01\xc9" ) ],
              [ "another-key", 124, 457 ],
              '->decode_data' );
}

done_testing;
