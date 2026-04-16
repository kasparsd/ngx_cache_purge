#!/usr/bin/env perl

use strict;
use warnings;

use File::Basename qw(dirname);
use File::Path qw(make_path remove_tree);
use FindBin;
use Getopt::Long qw(GetOptions);
use JSON::PP ();
use LWP::UserAgent;
use POSIX qw(strftime);
use Time::HiRes qw(sleep);

use lib "$FindBin::Bin/lib";
use Bench qw(fetch_stats format_table stats_delta write_json);

my %options = (
    count       => 1000,
    concurrency => 50,
    out_dir     => "$FindBin::Bin/results",
    port        => 18080,
);

GetOptions(
    'count=i'       => \$options{count},
    'config-template=s' => \$options{config_template},
    'concurrency=i' => \$options{concurrency},
    'out-dir=s'     => \$options{out_dir},
    'port=i'        => \$options{port},
    'quick'         => \$options{quick},
    'scenarios=s'   => \$options{scenarios},
) or die "invalid arguments\n";

die "--count must be > 0\n" unless $options{count} > 0;
die "--concurrency must be > 0\n" unless $options{concurrency} > 0;

my $duration = $options{quick} ? 15 : 60;
my $perl = $^X;
my $nginx = '/opt/nginx/sbin/nginx';
my $nginx_prefix = '/tmp/bench_nginx';
my $get_worker = "$FindBin::Bin/worker_get.pl";
my $purge_worker = "$FindBin::Bin/worker_purge.pl";
my $redis_pid_file = '/tmp/bench_redis.pid';
my $redis_port = 16379;
my $redis_db = 5;
my $stats_url = "http://127.0.0.1:$options{port}/bench_stats";
my $ua = LWP::UserAgent->new(agent => 'ngx-cache-purge-bench/1.0', keep_alive => 5, timeout => 5);

die "nginx binary not found at $nginx; run make nginx-build first\n"
    unless -x $nginx;

my %config_templates = discover_config_templates($FindBin::Bin);
my $override_template = defined $options{config_template}
    ? resolve_config_template(\%config_templates, $options{config_template})
    : undef;

my @all_scenarios = (
    {
        key        => 'exact',
        name       => 'exact_purge',
        table_name => 'exact_purge',
        config_template => 'nginx',
        prefix     => '/exact/',
        mode       => 'exact',
        backend    => 'sqlite',
    },
    {
        key        => 'wild',
        name       => 'wildcard_purge',
        table_name => 'wildcard_purge',
        config_template => 'nginx',
        prefix     => '/wild/',
        mode       => 'wildcard',
        backend    => 'sqlite',
    },
    {
        key        => 'tag-sqlite',
        name       => 'tag_sqlite_purge',
        table_name => 'tag_sqlite_purge',
        config_template => 'nginx',
        prefix     => '/tag/',
        mode       => 'tag',
        tag_base   => 'bench-tag',
        backend    => 'sqlite',
    },
    {
        key        => 'tag-redis',
        name       => 'tag_redis_purge',
        table_name => 'tag_redis_purge',
        config_template => 'nginx_redis',
        prefix     => '/rtag/',
        mode       => 'tag',
        tag_base   => 'bench-rtag',
        backend    => 'redis',
    },
);

my %scenario_by_key = map { $_->{key} => $_ } @all_scenarios;
my @selected_keys = defined $options{scenarios}
    ? split(/,/, $options{scenarios})
    : map { $_->{key} } @all_scenarios;

my @scenarios;
for my $key (@selected_keys) {
    die "unknown scenario: $key\n" unless exists $scenario_by_key{$key};
    my %scenario = %{ $scenario_by_key{$key} };
    $scenario{config_template_path} = defined $override_template
        ? $override_template->{path}
        : resolve_config_template(\%config_templates, $scenario{config_template})->{path};
    $scenario{config_template_name} = defined $override_template
        ? $override_template->{name}
        : $scenario{config_template};
    push @scenarios, \%scenario;
}

my $current_conf;
my $redis_started = 0;

END {
    eval { stop_nginx($current_conf) if defined $current_conf; };
    eval { stop_redis() if $redis_started; };
}

prepare_output_dir($options{out_dir});
cleanup_runtime_state();

my $timestamp = strftime('%Y%m%d-%H%M%S', localtime());
my $run_dir = "$options{out_dir}/$timestamp";
make_path($run_dir);

my @results;
my $active_runtime;

for my $scenario (@scenarios) {
    my $runtime_key = join("\0",
        $scenario->{config_template_path},
        $scenario->{backend},
    );

    if (!defined $active_runtime || $active_runtime ne $runtime_key) {
        stop_nginx($current_conf) if defined $current_conf;
        $current_conf = undef;

        if ($redis_started) {
            stop_redis();
            $redis_started = 0;
        }

        cleanup_runtime_state();

        if ($scenario->{backend} eq 'redis') {
            start_redis();
            $redis_started = 1;
        }

        $current_conf = render_runtime_config($scenario);

        start_nginx($current_conf);
        wait_for_stats($stats_url);
        $active_runtime = $runtime_key;
    }

    my $result = run_scenario($scenario, $duration, $run_dir, $stats_url);
    push @results, $result;
}

stop_nginx($current_conf) if defined $current_conf;
$current_conf = undef;

if ($redis_started) {
    stop_redis();
    $redis_started = 0;
}

my $summary = {
    generated_at => $timestamp,
    quick        => $options{quick} ? JSON::PP::true : JSON::PP::false,
    duration_s   => $duration,
    count        => $options{count},
    concurrency  => $options{concurrency},
    port         => $options{port},
    scenarios    => \@results,
};

write_json("$run_dir/summary.json", $summary);
my $table = format_table(\@results);
open my $summary_fh, '>', "$run_dir/summary.txt" or die "open(summary.txt): $!";
print {$summary_fh} $table or die "write(summary.txt): $!";
close $summary_fh or die "close(summary.txt): $!";

update_latest_symlink($options{out_dir}, $timestamp);
print $table;
print_summary_json($summary);

sub run_scenario {
    my ($scenario, $duration_s, $run_dir, $stats_endpoint) = @_;

    my $before = fetch_stats($stats_endpoint);
    warm_cache($scenario->{prefix}, $options{count});

    my $get_out = "$run_dir/$scenario->{name}_get.json";
    my $purge_out = "$run_dir/$scenario->{name}_purge.json";

    my $get_pid = fork_exec(
        $perl,
        $get_worker,
        '--scenario', $scenario->{name} . '_get',
        '--prefix', $scenario->{prefix},
        '--count', $options{count},
        '--concurrency', $options{concurrency},
        '--duration', $duration_s,
        '--port', $options{port},
        '--out', $get_out,
    );

    sleep(2);

    my @purge_command = (
        $perl,
        $purge_worker,
        '--scenario', $scenario->{name},
        '--mode', $scenario->{mode},
        '--prefix', $scenario->{prefix},
        '--count', $options{count},
        '--duration', $duration_s,
        '--port', $options{port},
        '--out', $purge_out,
    );

    if (defined $scenario->{tag_base}) {
        push @purge_command, '--tag-base', $scenario->{tag_base};
    }

    my $purge_pid = fork_exec(@purge_command);

    wait_for_child($get_pid, 'GET worker');
    wait_for_child($purge_pid, 'PURGE worker');

    my $after = fetch_stats($stats_endpoint);
    my $get_result = read_json($get_out);
    my $purge_result = read_json($purge_out);
    my $metrics_delta = stats_delta($before, $after);

    my $scenario_result = {
        scenario      => $scenario->{key},
        name          => $scenario->{name},
        table_name    => $scenario->{table_name},
        backend       => $scenario->{backend},
        config_template => $scenario->{config_template_name},
        duration_s    => $duration_s,
        get           => $get_result,
        purge         => $purge_result,
        stats_before  => $before,
        stats_after   => $after,
        metrics_delta => $metrics_delta,
    };

    write_json("$run_dir/$scenario->{name}.json", $scenario_result);
    unlink $get_out or die "unlink($get_out): $!";
    unlink $purge_out or die "unlink($purge_out): $!";

    return $scenario_result;
}

sub prepare_output_dir {
    my ($out_dir) = @_;
    make_path($out_dir) unless -d $out_dir;
}

sub cleanup_runtime_state {
    for my $path (
        '/tmp/bench_cache_exact',
        '/tmp/bench_cache_wild',
        '/tmp/bench_cache_tag_sqlite',
        '/tmp/bench_cache_tag_redis',
        '/tmp/bench_proxy_temp',
        '/tmp/bench_client_body_temp',
        $nginx_prefix,
    ) {
        remove_tree($path, { error => \my $error });
        if ($error && @{$error}) {
            for my $entry (@{$error}) {
                my ($failed_path, $message) = %{$entry};
                die "remove_tree($failed_path): $message\n" if defined $message;
            }
        }
    }

    for my $file (
        '/tmp/bench_tags.db',
        '/tmp/bench_tags.db-shm',
        '/tmp/bench_tags.db-wal',
        '/tmp/bench_nginx.pid',
        '/tmp/bench_nginx_error.log',
        '/tmp/bench_redis.log',
        $redis_pid_file,
    ) {
        unlink $file if -e $file;
    }

    unlink $_ for glob('/tmp/bench_nginx_*.conf');

    make_path(
        '/tmp/bench_proxy_temp',
        '/tmp/bench_client_body_temp',
        $nginx_prefix,
        "$nginx_prefix/logs",
    );
    ensure_nginx_prefix();
}

sub render_runtime_config {
    my ($scenario) = @_;

    my $backend = $scenario->{backend};
    my $template_name = sanitize_config_name($scenario->{config_template_name});
    my $target = "/tmp/bench_nginx_${template_name}_${backend}.conf";

    render_config($scenario->{config_template_path}, $target, $options{port});

    return $target;
}

sub render_config {
    my ($source, $target, $port) = @_;

    open my $in, '<', $source or die "open($source): $!";
    local $/;
    my $config = <$in>;
    close $in or die "close($source): $!";

    $config =~ s/listen\s+18080;/listen $port;/;

    open my $out, '>', $target or die "open($target): $!";
    print {$out} $config or die "write($target): $!";
    close $out or die "close($target): $!";
}

sub start_nginx {
    my ($conf) = @_;

    ensure_nginx_prefix();
    run_system($nginx, '-p', $nginx_prefix, '-c', $conf);
}

sub stop_nginx {
    my ($conf) = @_;

    return unless defined $conf;
    return unless -e '/tmp/bench_nginx.pid';

    ensure_nginx_prefix();
    system($nginx, '-p', $nginx_prefix, '-c', $conf, '-s', 'stop');
    sleep(0.2);
    unlink '/tmp/bench_nginx.pid' if -e '/tmp/bench_nginx.pid';
}

sub ensure_nginx_prefix {
    make_path($nginx_prefix, "$nginx_prefix/logs");

    my $error_log = "$nginx_prefix/logs/error.log";
    return if -e $error_log;

    open my $fh, '>', $error_log or die "open($error_log): $!";
    close $fh or die "close($error_log): $!";
}

sub wait_for_stats {
    my ($url) = @_;

    for (1 .. 100) {
        eval {
            my $payload = fetch_stats($url);
            die "stats payload missing zones\n" unless ref($payload->{zones}) eq 'HASH';
        };
        return if !$@;
        sleep(0.2);
    }

    die "nginx did not become ready at $url\n";
}

sub warm_cache {
    my ($prefix, $count) = @_;

    for my $index (0 .. ($count - 1)) {
        my $response = $ua->get("http://127.0.0.1:$options{port}$prefix$index");
        die "warm-up failed for $prefix$index: " . $response->status_line . "\n"
            unless $response->is_success;
    }

    sleep(0.5);
}

sub fork_exec {
    my @command = @_;
    my $pid = fork();
    die "fork failed: $!\n" unless defined $pid;

    if ($pid == 0) {
        exec @command or die "exec(@command): $!\n";
    }

    return $pid;
}

sub wait_for_child {
    my ($pid, $label) = @_;
    waitpid($pid, 0);
    die "$label failed\n" if $? != 0;
}

sub read_json {
    my ($file) = @_;

    open my $fh, '<', $file or die "open($file): $!";
    local $/;
    my $json = <$fh>;
    close $fh or die "close($file): $!";

    return JSON::PP::decode_json($json);
}

sub update_latest_symlink {
    my ($out_dir, $timestamp) = @_;

    my $latest = "$out_dir/latest";
    unlink $latest if -l $latest || -e $latest;
    symlink($timestamp, $latest) or die "symlink($latest): $!";
}

sub print_summary_json {
    my ($summary) = @_;

    print "\nSummary JSON\n";
    print JSON::PP->new->ascii->canonical->pretty->encode($summary);
}

sub start_redis {
    stop_redis() if -e $redis_pid_file;

    run_system(
        'redis-server',
        '--port', $redis_port,
        '--daemonize', 'yes',
        '--save', '',
        '--loglevel', 'warning',
        '--pidfile', $redis_pid_file,
        '--logfile', '/tmp/bench_redis.log',
    );

    for (1 .. 50) {
        my $status = system('redis-cli', '-p', $redis_port, 'PING');
        if ($status == 0) {
            run_system('redis-cli', '-p', $redis_port, '-n', $redis_db, 'FLUSHDB');
            return;
        }
        sleep(0.2);
    }

    die "redis-server did not become ready on port $redis_port\n";
}

sub stop_redis {
    return unless -e $redis_pid_file;

    system('redis-cli', '-p', $redis_port, 'shutdown', 'nosave');
    sleep(0.2);
    unlink $redis_pid_file if -e $redis_pid_file;
}

sub run_system {
    my @command = @_;
    system(@command) == 0 or die "command failed: @command\n";
}

sub discover_config_templates {
    my ($bench_dir) = @_;
    my %templates;

    for my $path (glob("$bench_dir/*.conf")) {
        my ($name) = $path =~ m{/([^/]+)\.conf$};
        next unless defined $name;

        $templates{$name} = {
            name => $name,
            path => $path,
        };
    }

    die "no benchmark config templates found under $bench_dir\n"
        unless %templates;

    return %templates;
}

sub resolve_config_template {
    my ($templates, $value) = @_;

    if (exists $templates->{$value}) {
        return $templates->{$value};
    }

    if (-f $value) {
        my ($name) = $value =~ m{/([^/]+)\.conf$};
        $name = defined $name ? $name : $value;
        return {
            name => $name,
            path => $value,
        };
    }

    die "unknown config template $value\n";
}

sub sanitize_config_name {
    my ($value) = @_;
    $value =~ s/[^A-Za-z0-9_-]+/_/g;
    return $value;
}