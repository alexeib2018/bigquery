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

my $dbh = DBI->connect("dbi:Pg:dbname=$dbname;host=$dbhost;port=$dbport;options=$dboptions;tty=$dbtty","$dbuser","$dbpass",
        {PrintError => 0});


sub select_csv {
    my $query = shift;

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
        push @row, '"'.$value.'"';
      }
      push @result, join ',', @row;
    }

    $sth->finish();

    (join "\n", @result) . "\n"
}


my $query = "

  SELECT campaign,
         installs,
         uninstalls,
         installs - uninstalls AS net_installs
  FROM (
    SELECT campaign,
           COUNT(*) AS installs
    FROM (
      SELECT user_pseudo_id,
             traffic_source ->> 'name' AS campaign
      FROM events
      GROUP BY user_pseudo_id, traffic_source ->> 'name'
    ) AS installs_table
    GROUP BY campaign
  ) AS installs_total
  JOIN (
    SELECT traffic_source ->> 'name' AS campaignu,
           COUNT(*) AS uninstalls
    FROM events
    WHERE event_name='app_remove'
    GROUP BY traffic_source ->> 'name'
  ) AS uninstalls_total
  ON installs_total.campaign = uninstalls_total.campaignu
  ORDER BY campaign

";

print "Content-type: text/csv\n";
print 'Content-Disposition: attachment; filename="campaigns.csv"'."\n";
print "\n";
print "# Title: Campaigns\n";
print "Campaign, Installs, Uninstalls, Net installs\n";
print select_csv($query);

$dbh->disconnect();
