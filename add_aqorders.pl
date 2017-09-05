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

my $destinationdb = "koha_siilinjarvi";
my $sourcedb = "siilinjarvi_temp";
my $sourcetable = "hankinta";

# PERL DBI CONNECT
my $dbh = DBI->connect($dsn_temp, $user, $pw);

my $today = C4::Dates->new();


$dbh->do("set foreign_key_checks = 0;");
my $start = time();

# $dbh->do("truncate ".$destinationdb.".aqorders;");
#$dbh->do("truncate ".$destinationdb.".aqbudgets;");
# $dbh->do("truncate ".$destinationdb.".aqbasket;");
# $dbh->do("truncate ".$destinationdb.".aqorders_items;");
# $dbh->do("delete from ".$destinationdb.".items where barcode is null;");

my $orders=$dbh->prepare("SELECT 
tauluID,
TeosID,
ToimittajaID,
Tilausaika,
Piste,
Osasto,
Hinta,
Lukumaara,
Alennusprosentti,
Alvprosentti FROM ".$sourcedb.".".$sourcetable." where Del = 'False' and Tilattu = 'True';");
$orders->execute;

my $row = 0;

while (my $order = $orders->fetchrow_hashref){
    my $biblio = get_biblionumber($order->{TeosID});
    $order->{biblionumber} = $biblio->{biblionumber};
    $order->{biblioitemnumber} = $biblio->{biblioitemnumber};
    $order->{Piste} = library_change($order->{Piste});
    $order->{Osasto} = location_change($order->{Osasto});
    $order->{Materiaali} = itype_change($order->{Materiaali});
    $order->{booksellerid} = get_bookseller($order->{ToimittajaID});
    unless ($order->{booksellerid}) {
        print Dumper $order->{ToimittajaID};
    }
    my ($itemcallnumber, $itype) = get_itemcallnumber($order->{TeosID}, $order->{Piste}, $order->{Osasto});
    $order->{Signum} = $itemcallnumber;
    $order->{Materiaali} = itype_change($itype);
    my $budget = create_budgets("aqbudgets", $order);
    my $basket = create_baskets("aqbasket", $order);
    $order->{budget_id} = $budget;
    $order->{basketno} = $basket;
    my $itemnumbers = create_items("items",$order);
    #print Dumper $order;
    my $ordernumber = insert_orders("aqorders", $order);

    foreach my $itemnumber (@{$itemnumbers}) {
        create_aqorder_items("aqorders_items",$itemnumber, $ordernumber);
    }

}

my $end = time();
my $time = $end - $start;
print "Time: ".strftime("\%H:\%M:\%S", gmtime($time))."\n";


sub insert_orders {
    my ($tablename, $source) = @_;

    my $price = parse_data($source->{Hinta});
    $price =~ tr/,/./;

    my $rrp = $price/parse_decimals($source->{Alvprosentti}, '1');
    my $ecost = $price-($price*parse_decimals($source->{Alennusprosentti},'0'))/parse_decimals($source->{Alvprosentti}, '1');

    my @fields;

    push @fields, (
    'biblionumber',
    'entrydate',
    'currency',
    'quantity',
    'listprice',
    'rrp',
    'ecost',
    'gstrate',
    'discount',
    'budget_id',
    'basketno',
    'orderstatus');

    my $insert = "INSERT INTO ".$destinationdb.".".$tablename." (";
    foreach my $field (@fields) {
        if (\$field == \$fields[-1]) {
            $insert .= $field."";
        }else {
            $insert .= $field.",";
        }
        
    }
    $insert .= ") VALUES (";
    $insert .= "'".$source->{biblionumber}."',";
    $insert .= "'".parse_date($source->{Tilausaika})."',";
    $insert .= "'EUR',";
    $insert .= "'".parse_data($source->{Lukumaara})."',";
    $insert .= "'".$price."',";
    $insert .= "'".$rrp."',";
    $insert .= "'".$ecost."',";
    $insert .= "'".parse_decimals($source->{Alvprosentti}, '0')."',";
    $insert .= "'".parse_decimals($source->{Alennusprosentti}, '0')."',";
    $insert .= "'".parse_data($source->{budget_id})."',";
    $insert .= "'".parse_data($source->{basketno})."',";
    $insert .= "'ordered'";
    $insert .= ")";
    
    #print "$insert\n";
    $dbh->do($insert);

    return $dbh->{mysql_insertid};
   
}

sub create_items {
    my ($tablename,$source) = @_;

    my @itemnumbers;

    for (my $var = 0; $var < parse_data($source->{Lukumaara}); $var++) {

        my $price = parse_data($source->{Hinta});
        $price =~ tr/,/./;

        my @fields;

        push @fields, (
        'biblionumber',
        'biblioitemnumber',
        'dateaccessioned',
        'booksellerid',
        'homebranch',
        'price',
        'replacementprice',
        'datelastseen',
        'notforloan',
        'itemcallnumber',
        'holdingbranch',
        'location',
        'permanent_location',
        'itype');

        my $insert = "INSERT INTO ".$destinationdb.".".$tablename." (";
        foreach my $field (@fields) {
            if (\$field == \$fields[-1]) {
                $insert .= $field."";
            }else {
                $insert .= $field.",";
            }
            
        }
        $insert .= ") VALUES (";
        $insert .= "'".$source->{biblionumber}."',";
        $insert .= "'".$source->{biblioitemnumber}."',";
        $insert .= "'".parse_date($source->{Tilausaika})."',";
        $insert .= "'".parse_data($source->{Toimittaja})."',";
        $insert .= "'".parse_data($source->{Piste})."',";
        $insert .= "'".$price."',";
        $insert .= "'".$price."',";
        $insert .= "'".parse_date($source->{Tilausaika})."',";
        $insert .= "'-1',";
        $insert .= "'".parse_data($source->{Signum})."',";
        $insert .= "'".parse_data($source->{Piste})."',";
        $insert .= "'".parse_data($source->{Osasto})."',";
        $insert .= "'".parse_data($source->{Osasto})."',";
        $insert .= "'".parse_data($source->{Materiaali})."'";
        $insert .= ")";
        
        #print "$insert\n";
        $dbh->do($insert);
        push @itemnumbers, $dbh->{mysql_insertid};
    }
    return \@itemnumbers;
}

sub create_aqorder_items {
    my ($tablename, $itemnumber, $ordernumber) = @_;

    my $insert = "INSERT INTO ".$destinationdb.".".$tablename." (itemnumber, ordernumber) 
    VALUES (".$itemnumber.",".$ordernumber.")";
    #print "$insert\n";
    $dbh->do($insert);

}

sub create_budgets {
    my ($tablename, $source) = @_;

    my $query = "SELECT budget_id FROM ".$destinationdb.".".$tablename." WHERE budget_code = ? and budget_branchcode = ?";
    my $sth=$dbh->prepare($query);
    my $code = parse_data($source->{Piste})." ".parse_data($source->{Osasto})." 2016";
    $sth->execute($code, parse_data($source->{Piste}));
    my $budget = $sth->fetchrow;
    unless ($budget) {

        my @fields;

        push @fields, (
        'budget_code',
        'budget_name',
        'budget_branchcode',
        'budget_amount',
        'budget_period_id');

        my $insert = "INSERT INTO ".$destinationdb.".".$tablename." (";
        foreach my $field (@fields) {
            if (\$field == \$fields[-1]) {
                $insert .= $field."";
            }else {
                $insert .= $field.",";
            }
            
        }
        $insert .= ") VALUES (";
        $insert .= "'".parse_data($source->{Piste})." ".parse_data($source->{Osasto})." 2016',";
        $insert .= "'".parse_data($source->{Piste})." ".parse_data($source->{Osasto})." 2016',";
        $insert .= "'".parse_data($source->{Piste})."',";
        $insert .= "'20000.00',";
        $insert .= "'1'";
        $insert .= ")";
        
        #print "$insert\n";
        $dbh->do($insert);
        return $dbh->{mysql_insertid};
    } else {
        return $budget;
    }
}

sub create_baskets {
    my ($tablename, $source) = @_;

    my $query = "SELECT basketno FROM ".$destinationdb.".".$tablename." WHERE basketname = ? and creationdate = ?";
    my $sth=$dbh->prepare($query);
    my $code = parse_data($source->{Piste})." ".parse_data($source->{Osasto})." 2016";
    $sth->execute($code, parse_date($source->{Tilausaika}));
    my $basket = $sth->fetchrow;
    unless ($basket) {

        my @fields;

        push @fields, (
        'basketname',
        'creationdate',
        'closedate',
        'booksellerid',
        'deliveryplace',
        'billingplace',
        'branch');

        my $insert = "INSERT INTO ".$destinationdb.".".$tablename." (";
        foreach my $field (@fields) {
            if (\$field == \$fields[-1]) {
                $insert .= $field."";
            }else {
                $insert .= $field.",";
            }
            
        }
        $insert .= ") VALUES (";
        $insert .= "'".parse_data($source->{Piste})." ".parse_data($source->{Osasto})." 2016',";
        $insert .= "'".parse_date($source->{Tilausaika})."',";
        $insert .= "current_date,";
        $insert .= "'".parse_data($source->{booksellerid})."',";
        $insert .= "'".parse_data($source->{Piste})."',";
        $insert .= "'".parse_data($source->{Piste})."',";
        $insert .= "'".parse_data($source->{Piste})."'";
        $insert .= ")";
        
        #print "$insert\n";
        $dbh->do($insert);
        return $dbh->{mysql_insertid};
    } else {
        return $basket;
    }
}

sub create_budgetperiod {
    my ($tablename, $source) = @_;

    my @fields;

    push @fields, (
    'budget_period_startdate',
    'budget_period_enddate', 
    'budget_period_active', 
    'budget_period_description', 
    'budget_period_total');

    my $insert = "INSERT INTO ".$destinationdb.".".$tablename." (";
    foreach my $field (@fields) {
        if (\$field == \$fields[-1]) {
            $insert .= $field."";
        }else {
            $insert .= $field.",";
        }
        
    }
    $insert .= ") VALUES (";
    $insert .= "'".parse_data($source->{branchname})." 2016',";
    $insert .= "'".parse_data($source->{branchname})." 2016',";
    $insert .= "'".parse_data($source->{branchcode})."',";
    $insert .= "'".parse_data($source->{Erapaiva})."',";
    $insert .= "'".parse_data($source->{Uusimiskerta})."',";
    $insert .= "'".parse_data($source->{Lainausaika})."',";
    $insert .= "'".parse_data($source->{Lainauspiste})."'";
    $insert .= ")";
    
    #print "$insert\n";

}

sub get_biblionumber {
    my ($id) = @_;
    $id =~ s/\x00//g;
    my $query = "SELECT biblionumber, biblioitemnumber FROM ".$destinationdb.".biblioitems where collectiontitle = ?;";
    my $sth=$dbh->prepare($query);
    $sth->execute($id);

    return $sth->fetchrow_hashref;

}

sub get_itemcallnumber {
    my ($id, $branch, $loc) = @_;
    $id =~ s/\x00//g;
    $id =~ s/{//g;
    $id =~ s/}//g;
    my $query = "SELECT Luokka, Paasana, Materiaali FROM ".$sourcedb.".teokset_uusi where tauluID = ?;";
    my $sth=$dbh->prepare($query);
    $sth->execute($id);

    my $data = $sth->fetchrow_hashref;

    my $itemcallnumber = set_itemcallnumber(parse_data($data->{Luokka}), parse_data($data->{Paasana}), parse_data($branch), parse_data($loc));

    return $itemcallnumber, parse_data($data->{Materiaali});
}

sub set_itemcallnumber {
    my ($luokka, $paasana, $piste, $osasto) = @_;
    my $itemcallnumber;
    my $new_piste = substr($piste, 4, index($piste, '_'));
    my $new_paasana = substr($paasana, 0, 3);
    $itemcallnumber = $luokka." ".$new_paasana." ".$new_piste." ".$osasto;
    return $itemcallnumber;
}

sub get_bookseller {
    my ($id) = @_;
    $id =~ s/\x00//g;
    my $query = "SELECT k.Yhteisonimi, k.Sahkoposti, t.Asiakasnumero FROM ".$sourcedb.".toimittajat t 
    join ".$sourcedb.".kontakti k on t.KontaktiID = k.tauluID where t.tauluID = ?;";
    my $sth=$dbh->prepare($query);
    $sth->execute($id);
    my $bookseller = $sth->fetchrow_hashref;
    my $query1 = "SELECT id FROM ".$destinationdb.".aqbooksellers WHERE name = ?";
    my $sth1=$dbh->prepare($query1);
    $sth1->execute(parse_data($bookseller->{Yhteisonimi}));
    return $sth1->fetchrow;

}

sub location_change {
    my ($location) = @_;

    $location = parse_data($location);
    my $query = "SELECT lyhenne FROM ".$sourcedb.".osastot WHERE Id = ?";
    my $sth=$dbh->prepare($query);
    $sth->execute($location);

    return $sth->fetchrow;
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

sub parse_decimals {
    my ($data, $decimal) = @_;
    my $length = length parse_data($data);
    my $f;
    if ($length eq '1') {
        $f = $decimal.".0".parse_data($data);
        return $f;
    }
    if ($length eq '2') {
        $f = $decimal.".".parse_data($data);
        return $f;
    }
    return parse_data($data);
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

sub itype_change {
    my ($type) = @_;
    $type = parse_data($type);
    my $new_type;
    my $desc;
    if ($type eq "2") {
        $new_type = "AANINAUHA";
        $desc = "Ääninauha";
    }#Ääninauha
    if ($type eq "3") {
        $new_type = "FILMIK";
        $desc = "Filmikela";
    }#Filmikela
    if ($type eq "4") {
        $new_type = "DVDROM";
        $desc = "DVD-ROM";
    }#DVD-ROM -levy
    if ($type eq "5") {
        $new_type = "MIKROK";
        $desc = "Mikrokortti";
    }#Mikrokortti
    if ($type eq "6") {
        $new_type = "KOOSTE";
        $desc = "Kooste";
    }#Kooste
    if ($type eq "13") {
        $new_type = "AANICD";
        $desc = "Äänikirja(CD)";
    }#Äänikirja(CD)
    if ($type eq "14") {
        $new_type = "KONSOLIP";
        $desc = "Konsolipeli";
    }#Konsolipeli
    if ($type eq "15") {
        $new_type = "BDLEVY";
        $desc = "BD-levy";
    }#BD-levy
    if ($type eq "16") {
        $new_type = "AANIKMP3";
        $desc = "Äänikirja(MP3)";
    }#Äänikirja(mp3)
    if ($type eq "17") {
        $new_type = "LIIKUNTA";
        $desc = "Liikuntaväline";
    }#Liikuntaväline
    if ($type eq "18") {
        $new_type = "CELIA";
        $desc = "Celia äänikirja";
    }#Celian äänikirja
    if ($type eq "19") {
        $new_type = "EKIRJA";
        $desc = "E-kirja";
    }#E-kirja
    if ($type eq "100001") {
        $new_type = "KIRJA";
        $desc = "Kirja";
    }#Kirja
    if ($type eq "100003") {
        $new_type = "PIENPAI";
        $desc = "Pienpainate";
    }#Pienpainate
    if ($type eq "100004") {
        $new_type = "MONISTE";
        $desc = "Moniste";
    }#Moniste
    if ($type eq "100005") {
        $new_type = "TYOPIIR";
        $desc = "Työpiirustus";
    }#Työpiirustus
    if ($type eq "100006") {
        $new_type = "AANIKIRJA";
        $desc = "Äänikirja";
    }#Äänikirja
    if ($type eq "100007") {
        $new_type = "MONIV";
        $desc = "Moniviestin";
    }#Moniviestin
    if ($type eq "100011") {
        $new_type = "LEHTI";
        $desc = "Lehti";
    }#Lehti
    if ($type eq "100012") {
        $new_type = "KARTTA";
        $desc = "Kartta";
    }#Kartta
    if ($type eq "100013") {
        $new_type = "NUOTTI";
        $desc = "Nuotti";
    }#Nuotti
    if ($type eq "100015") {
        $new_type = "CD";
        $desc = "CD-levy";
    }#CD-levy
    if ($type eq "100016") {
        $new_type = "MDLEVY";
        $desc = "MD-levy";
    }#MD-levy
    if ($type eq "100017") {
        $new_type = "AANILEVY";
        $desc = "Äänilevy";
    }#Äänilevy
    if ($type eq "100018") {
        $new_type = "KASETTI";
        $desc = "Kasetti";
    }#Kasetti
    if ($type eq "100021") {
        $new_type = "VIDEO";
        $desc = "Videokasetti";
    }#Videokasetti
    if ($type eq "100022") {
        $new_type = "LASERKUVLEV";
        $desc = "Laserkuvalevy";
    }#Laserkuvalevy
    if ($type eq "100023") {
        $new_type = "DIA";
        $desc = "Dia";
    }#Dia
    if ($type eq "100024") {
        $new_type = "KUVA";
        $desc = "Kuva";
    }#Kuva
    if ($type eq "100025") {
        $new_type = "ESINE";
        $desc = "Esine";
    }#Esine
    if ($type eq "100027") {
        $new_type = "CDROM";
        $desc = "CD-ROM";
    }#CD-ROM
    if ($type eq "100029") {
        $new_type = "DVD";
        $desc = "DVD-levy";
    }#DVD-levy
    if ($type eq "100031") {
        $new_type = "KASIK";
        $desc = "Käsikirjoitus";
    }#Käsikirjoitus
    if ($type eq "100032") {
        $new_type = "MIKROF";
        $desc = "Mikrofilmi";
    }#Mikrofilmi
    return $new_type;
}

sub encode_to {
    my ( $value ) = @_;
    return encode( 'iso-8859-1', $value);
}