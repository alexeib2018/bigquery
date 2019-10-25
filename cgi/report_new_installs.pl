#!/usr/bin/env perl

use strict;
use warnings;
use DBI;
# use Data::Dumper;


require "settings.pl";
our $dbhost;
our $dbname;
our $dbuser;
our $dbpass;

my $dbport = "5432";
my $dboptions = "-e";
my $dbtty = "ansi";


sub select_json {
    my $query = shift;

    my $dbh = DBI->connect("dbi:Pg:dbname=$dbname;host=$dbhost;port=$dbport;options=$dboptions;tty=$dbtty","$dbuser","$dbpass",
            {PrintError => 0});

    my $sth = $dbh->prepare($query);
    my $rv = $sth->execute();
    if (!defined $rv) {
      print "Error in request: " . $dbh->errstr . "\n";
      exit(0);
    }

    my $fields = $sth->{NAME};

    # my $types = $sth->{TYPE};
    # print Dumper($types);

    my @result=();
    while (my @array = $sth->fetchrow_array()) {
      my @row=();
      for(my $i=0; $i<scalar @$fields; $i++) {
        my $field = @$fields[$i];
        my $value = $array[$i];
        $value =~ s/"/\\"/g;
        push @row, '"'.$field.'":"'.$value.'"';
      }
      push @result, '{'.(join ',', @row).'}';
    }

    $sth->finish();
    $dbh->disconnect();

    '['.(join ',', @result).']'
}


print "Content-type: application/json\n\n";

my $query = "
  SELECT date,
         COUNT(*) AS count
  FROM (SELECT DATE(user_first_touch_timestamp) AS date,
               user_pseudo_id
        FROM events
        WHERE user_first_touch_timestamp>'2019-10-11'
        GROUP BY user_first_touch_timestamp, user_pseudo_id) AS installs
  GROUP BY date
  ORDER BY date;
";

print select_json($query);
