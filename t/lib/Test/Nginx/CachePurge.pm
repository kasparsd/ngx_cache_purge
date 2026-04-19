package Test::Nginx::CachePurge;

use strict;
use warnings;

use Test::Nginx::Socket -Base;

sub cache_http_config {
    if (@_ % 2 == 1) {
        shift @_;
    }

    my (%args) = @_;

    my $cache_path = $args{cache_path} // '/tmp/ngx_cache_pilot_cache';
    my $zone = $args{zone} // 'test_cache';
    my $zone_size = $args{zone_size} // '10m';
    my $temp_path = $args{temp_path} // '/tmp/ngx_cache_pilot_temp';
    my $temp_levels = $args{temp_levels} // '1 2';

    my @lines = (
        "    proxy_cache_path  $cache_path keys_zone=$zone:$zone_size;",
        "    proxy_temp_path   $temp_path $temp_levels;",
    );

    if ($args{include_purge_method_map}) {
        push @lines,
            '    map $request_method $purge_method {',
            '        PURGE   1;',
            '        default 0;',
            '    }';
    }

    if ($args{include_purge_never_map}) {
        push @lines,
            '    map $request_method $purge_never {',
            '        default 0;',
            '    }';
    }

    if (defined $args{index_zone_size}) {
        push @lines, "    cache_pilot_index_zone_size $args{index_zone_size};";
    }

    return join("\n", @lines) . "\n";
}

sub add_default_block_config {
    if (@_ % 2 == 1) {
        shift @_;
    }

    my (%args) = @_;

    add_block_preprocessor(sub {
        my $block = shift;

        if (!defined $block->config && defined $args{config}) {
            $block->set_value('config', $args{config});
        }

        if (!defined $block->timeout && defined $args{timeout}) {
            $block->set_value('timeout', $args{timeout});
        }
    });
}

sub set_default_http_config {
    if (@_ % 2 == 1) {
        shift @_;
    }

    my (%args) = @_;
    my $default_http_config = $args{http_config};

    Test::Nginx::Socket::set_http_config_filter(sub {
        my $http_config = shift;

        $http_config = '' if !defined $http_config;
        return $http_config if !defined $default_http_config || $default_http_config eq '';

        return $default_http_config . $http_config;
    });
}

1;