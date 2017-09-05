#!/usr/bin/perl

use strict;
#use warnings;
use DBI;
use DBD::mysql;
use Data::Dumper;
use File::Basename;
use POSIX qw{strftime};
use C4::Members;

# CONFIG VARIABLES
my $user = "user";
my $pw = "password";

# DATA SOURCE NAME

my $dsn_temp = "dbi:mysql::10.0.3.12:3306";

my $destinationdb = "koha";
my $sourcedb = "temp";

# PERL DBI CONNECT
my $dbh = DBI->connect($dsn_temp, $user, $pw);

my $today = C4::Dates->new();


$dbh->do("set foreign_key_checks = 0;");
# $dbh->do("truncate ".$destinationdb.".reserves;");
my $start = time();

my $reserves=$dbh->prepare("SELECT 
tauluID,
AsiakasID,
Alkamispaiva,
Noutopiste,
Lisatiedot,
ViimeinenNoutoPaiva FROM ".$sourcedb.".varaukset where Del = 'False';");
$reserves->execute;

my $row = 0;

while (my $reserve = $reserves->fetchrow_hashref){
    my $reserverow = get_reserverows($reserve->{tauluID});
    $reserve->{biblionumber} = get_biblionumber($reserverow->{TeosID});
    $reserve->{borrowernumber} = get_borrower($reserve->{AsiakasID});
    $reserve->{Noutopiste} = library_change($reserve->{Noutopiste});
    $reserve->{Jarjestys} = $reserverow->{Jarjestys};
    #print Dumper $reserve;
    if ($reserve->{biblionumber} && $reserve->{borrowernumber}) {
        insert_reserves("reserves", $reserve);
    }

}

my $end = time();
my $time = $end - $start;
print "Time: ".strftime("\%H:\%M:\%S", gmtime($time))."\n";


sub insert_reserves {
    my ($tablename, $source) = @_;

    my $waiting = parse_date($source->{ViimeinenNoutoPaiva});
    if ($waiting eq '--') {
        $waiting = '';
    }
    print Dumper $waiting;

    my @fields;

    if ($waiting) {
        push @fields, ('waitingdate');
    }

    push @fields, (
    'borrowernumber',
    'reservedate',
    'biblionumber',
    'constrainttype',
    'branchcode',
    'reservenotes',
    'priority');

    my $insert = "INSERT INTO ".$destinationdb.".".$tablename." (";
    foreach my $field (@fields) {
        if (\$field == \$fields[-1]) {
            $insert .= $field."";
        }else {
            $insert .= $field.",";
        }
        
    }
    $insert .= ") VALUES (";
    if($waiting) {
       $insert .= "'".$waiting."',"; 
    }
    $insert .= "'".$source->{borrowernumber}."',";
    $insert .= "'".parse_date($source->{Alkamispaiva})."',";
    $insert .= "'".parse_data($source->{biblionumber})."',";
    $insert .= "'a',";
    $insert .= "'".parse_data($source->{Noutopiste})."',";
    $insert .= "'".parse_data($source->{Lisatiedot})."',";
    $insert .= "'".parse_data($source->{Jarjestys})."'";
    $insert .= ")";
    
    #print "$insert\n";
    $dbh->do($insert);

    #return $dbh->{mysql_insertid};
   
}

sub get_biblionumber {
    my ($id) = @_;
    $id =~ s/\x00//g;
    my $query = "SELECT biblionumber FROM ".$destinationdb.".biblioitems where collectiontitle = ?;";
    my $sth=$dbh->prepare($query);
    $sth->execute($id);

    return $sth->fetchrow;

}

sub get_reserverows {
    my ($id) = @_;
    $id =~ s/\x00//g;
    my $query = "SELECT TeosID, Jarjestys FROM ".$sourcedb.".varausrivit where VarausID = ?";
    my $sth=$dbh->prepare($query);
    $sth->execute($id);
    return $sth->fetchrow_hashref;

}

sub get_borrower {
    my ($id) = @_;
    $id =~ s/\x00//g;
    my $query = "SELECT tauluID FROM ".$sourcedb.".asiakasviivakoodit where AsiakasID = ?";
    my $sth=$dbh->prepare($query);
    $sth->execute($id);
    my $cardnumber = parse_data($sth->fetchrow);

    my $query2 = "SELECT borrowernumber FROM ".$destinationdb.".borrowers where cardnumber = ?";
    my $sth2=$dbh->prepare($query2);
    $sth2->execute($cardnumber);
    my $data = $sth2->fetchrow;
    unless ($data) {
        my $query1 = "SELECT Sukunimi, Etunimet, Syntymaaika FROM ".$sourcedb.".asiakkaat a 
        JOIN ".$sourcedb.".kontakti k ON a.KontaktiID = k.tauluID 
        where a.tauluID = ?";
        my $sth1=$dbh->prepare($query1);
        $sth1->execute($id);
        my $asiakas = $sth1->fetchrow_hashref;
        my $query3 = "SELECT borrowernumber FROM ".$destinationdb.".borrowers where surname = ? and firstname = ? and dateofbirth = ?";
        my $sth3=$dbh->prepare($query3);
        $sth3->execute(parse_data($asiakas->{Sukunimi}), parse_data($asiakas->{Etunimet}), parse_date($asiakas->{Syntymaaika}));
        my $borr = $sth3->fetchrow;
        print "$borr\n";
        return $borr;
    } else {
        return $data;
    }

    # my $query2 = "SELECT borrowernumber FROM ".$destinationdb.".borrowers where cardnumber = ?";
    # my $sth2=$dbh->prepare($query2);
    # $sth2->execute($cardnumber);
    # return $sth2->fetchrow;

}

sub library_change {
    my ($branch) = @_;

    $branch = parse_data($branch);
    if ($branch eq '1') {
        return "SII_PK";
    }
    if ($branch eq '3') {
        return "SII_AU";
    }
    if ($branch eq '4') {
        return "SII_VU";
    }

    return $branch;
}

sub parse_data {
    my ($data) = @_;
    $data =~ s/\x00//g;
    return trim($data);
}

sub parse_date {
    my ($date) = @_;

    $date =~ s/\x00//g;
    my ($y, $m, $d) = $date =~ /^(\d\d\d\d)-(\d\d)-(\d\d)/;
    my $bdate = $y."-".$m."-".$d;

    return $bdate;
}

sub  trim { 
    my $s = shift; 
    $s =~ s/^\s+|\s+$//g; 
    return $s 
};