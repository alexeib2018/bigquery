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

SELECT user_pseudo_id,
       campaign,
       install_date,
       uninstall_date,
       days_used
FROM (
  SELECT user_pseudo_id,
         COUNT(*) AS days_used
  FROM (
    SELECT event_date,
           user_pseudo_id
    FROM events
    GROUP BY event_date, user_pseudo_id
  ) AS users_by_day
  GROUP BY user_pseudo_id
) AS days_used
JOIN (
  SELECT user_pseudo_id AS user_pseudo_idc,
         traffic_source ->> 'name' AS campaign
  FROM events
  GROUP BY user_pseudo_idc, campaign
) AS user_campaign
ON days_used.user_pseudo_id = user_campaign.user_pseudo_idc
JOIN (
  SELECT DATE(user_first_touch_timestamp) AS install_date,
         user_pseudo_id AS user_pseudo_idi
  FROM events
  WHERE user_first_touch_timestamp>='2019-10-11'
  GROUP BY user_first_touch_timestamp, user_pseudo_idi
) AS installs
ON days_used.user_pseudo_id = installs.user_pseudo_idi
LEFT JOIN (
  SELECT event_date AS uninstall_date,
         user_pseudo_id AS user_pseudo_idu
  FROM events
  WHERE event_name='app_remove'
  GROUP BY event_date, user_pseudo_idu
  ORDER BY event_date
) AS uninstalls
ON days_used.user_pseudo_id = uninstalls.user_pseudo_idu
ORDER BY days_used DESC

";

print "Content-type: text/csv\n";
print 'Content-Disposition: attachment; filename="days_used.csv"'."\n";
print "\n";
print "# Title: Days Used\n";
print 'User, Campaign, "Install date", "Uninstall date", "Days used"' . "\n";
print select_csv($query);

$dbh->disconnect();
