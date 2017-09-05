#!/usr/bin/perl

use strict;
use utf8;
#use warnings;
use DBI;
use DBD::mysql;
use Data::Dumper;
use File::Basename;
use POSIX qw{strftime};
use C4::Members;
use Encode;

# CONFIG VARIABLES
my $user = "user";
my $pw = "password";

# DATA SOURCE NAME

my $dsn_temp = "dbi:mysql::10.0.3.12:3306";

my $destinationdb = "koha";
my $sourcedb = "temp";
my $sourcetable = "niteet";

# PERL DBI CONNECT
my $dbh = DBI->connect($dsn_temp, $user, $pw);

my $today = C4::Dates->new();


$dbh->do("set foreign_key_checks = 0;");
# $dbh->do("truncate ".$destinationdb.".items;");
#$dbh->do("truncate ".$destinationdb.".aqbooksellers;");
# $dbh->do("truncate ".$destinationdb.".itemtypes;");
# $dbh->do("INSERT INTO ".$destinationdb.".itemtypes (itemtype, description) VALUES ('EKIRJA', 'E-kirja');");
my $start = time();

my $items=$dbh->prepare("SELECT Viivakoodi,
Hankintapvm,
ToimittajaId,
Kotipiste,
Hankintahinta,
Korvaushinta,
Havaintopvm,
Lainauskielto,
Kadonnut,
Luokka,
Lisatiedot,
Piste,
Osasto,
TeosID,
Tila,
Hylly,
VanhojenLainojenLkm  FROM ".$sourcedb.".".$sourcetable." where Del = '0';");
$items->execute;

my $row = 0;

while (my $item = $items->fetchrow_hashref){
    my $record= get_record($item->{TeosID});
    $item->{Materiaali} = itype_change($record->{Materiaali});
    $item->{Kotipiste} = library_change($item->{Kotipiste});
    $item->{Piste} = library_change($item->{Piste});
    $item->{Osasto} = location_change($item->{Osasto});
    $item->{Hylly} = ccode_change($item->{Hylly});
    $item->{Paasana} = $record->{Paasana};
    my $itemcallnumber = set_itemcallnumber(parse_data($item->{Luokka}), parse_data($item->{Paasana}), parse_data($item->{Kotipiste}), parse_data($item->{Osasto}));
    $item->{Signum} = $itemcallnumber;
    my $bookseller = get_bookseller($item->{ToimittajaId});
    $item->{Toimittaja} = $bookseller->{Yhteisonimi};
    my $biblio = get_biblionumber($item->{TeosID});
    $item->{biblionumber} = $biblio->{biblionumber};
    $item->{biblioitemnumber} = $biblio->{biblioitemnumber};
    
    $item->{subloc} = subloc_change(parse_data($item->{Tila}));
    if(parse_data($item->{Tila}) eq "9") {
        $item->{withdrawn} = "1";
    }

    if(parse_data($item->{Tila}) eq "3") {
        $item->{lost} = "1";
    }

    if(parse_data($item->{Tila}) eq "8") {
        $item->{damaged} = "2";
    }
    
    $item->{notforloan} = notforloan_change($item->{Tila});
    #print Dumper $item;
    insert_items("items", $item);

}

my $end = time();
my $time = $end - $start;
print "Time: ".strftime("\%H:\%M:\%S", gmtime($time))."\n";

sub insert_items {
    my ($tablename, $source) = @_;

    my $replacementprice = parse_data($source->{Korvaushinta});
    $replacementprice =~ tr/,/./;

    my $price = parse_data($source->{Hankintahinta});
    $price =~ tr/,/./;

    my @fields;

    push @fields, (
    'biblionumber',
    'biblioitemnumber',
    'barcode',
    'dateaccessioned',
    'booksellerid',
    'homebranch',
    'price',
    'replacementprice',
    'datelastseen',
    'notforloan',
    'itemlost',
    'itemcallnumber',
    'itemnotes_nonpublic',
    'holdingbranch',
    'location',
    'permanent_location',
    'itype',
    'ccode',
    'issues',
    'withdrawn',
    'sub_location',
    'damaged');

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
    $insert .= "'".parse_data($source->{Viivakoodi})."',";
    $insert .= "'".parse_date($source->{Hankintapvm})."',";
    $insert .= "'".parse_data($source->{Toimittaja})."',";
    $insert .= "'".parse_data($source->{Kotipiste})."',";
    $insert .= "'".$price."',";
    $insert .= "'".$replacementprice."',";
    $insert .= "'".parse_date($source->{Havaintopvm})."',";
    if (parse_data($source->{Lainauskielto}) eq "True") {
        $insert .= "'1',";
    } else {
        $insert .= "'".parse_data($source->{notforloan})."',";
    }
    if(parse_data($source->{lost})) {
        $insert .= "'".parse_data($source->{lost})."',";
    } else {
        $insert .= "'".set_boolean(parse_data($source->{Kadonnut}))."',";
    }
    
    $insert .= "'".parse_data($source->{Signum})."',";
    $insert .= "'".parse_data($source->{Lisatiedot})."',";
    $insert .= "'".parse_data($source->{Piste})."',";
    $insert .= "'".parse_data($source->{Osasto})."',";
    $insert .= "'".parse_data($source->{Osasto})."',";
    $insert .= "'".parse_data($source->{Materiaali})."',";
    $insert .= "'".parse_data($source->{Hylly})."',";
    $insert .= "'".parse_data($source->{VanhojenLainojenLkm})."',";
    $insert .= "'".parse_data($source->{withdrawn})."',";
    $insert .= "'".parse_data($source->{subloc})."',";
    $insert .= "'".parse_data($source->{damaged})."'";
    $insert .= ")";
    
    #print "$insert\n";
    $dbh->do($insert);

    return $dbh->{mysql_insertid};
   
}

sub get_record {
    my ($id) = @_;
    $id =~ s/\x00//g;
    $id =~ s/{//g;
    $id =~ s/}//g;
    my $query = "SELECT Materiaali, Paasana FROM ".$sourcedb.".teokset_uusi where tauluID = ?";
    my $sth=$dbh->prepare($query);
    $sth->execute($id);

    return $sth->fetchrow_hashref;

}

sub get_bookseller {
    my ($id) = @_;
    $id =~ s/\x00//g;
    my $query = "SELECT * FROM ".$sourcedb.".toimittajat t 
    join ".$sourcedb.".kontakti k on t.KontaktiID = k.tauluID where t.tauluID = ?;";
    my $sth=$dbh->prepare($query);
    $sth->execute($id);
    my $bookseller = $sth->fetchrow_hashref;
    # if (parse_data($bookseller->{Del}) eq "False") {
    #     print Dumper $bookseller;
    #     insert_booksellers("aqbooksellers", $bookseller);
    # }
    return $bookseller;

}

sub insert_booksellers {
    my ($tablename, $source) = @_;

    my $query = "SELECT name FROM ".$destinationdb.".aqbooksellers WHERE name = ? and address1 = ? and accountnumber = ?";
    my $sth=$dbh->prepare($query);
    $sth->execute(parse_data($source->{Yhteisonimi}), parse_data($source->{Sahkoposti}), parse_data($source->{Asiakasnumero}));
    unless (my $row = $sth->fetchrow) {

        my $address = get_address($source->{KontaktiID}, 0);
        my $discount = parse_data($source->{AlennusProsentti});
        $discount =~ tr/,/./;

        my @fields;

        push @fields, (
        'name',
        'address1',
        'notes',
        'bookselleremail',
        'discount',
        'postal',
        'listprice',
        'invoiceprice',
        'accountnumber');

        my $insert = "INSERT INTO ".$destinationdb.".".$tablename." (";
        foreach my $field (@fields) {
            if (\$field == \$fields[-1]) {
                $insert .= $field."";
            }else {
                $insert .= $field.",";
            }
            
        }

        $insert .= ") VALUES (";
        $insert .= "'".parse_data($source->{Yhteisonimi})."',";
        $insert .= "'".parse_data($source->{Sahkoposti})."',";
        $insert .= "'".parse_data($source->{Lisatiedot})."',";
        $insert .= "'".parse_data($source->{Sahkoposti})."',";
        $insert .= "'".$discount."',";
        $insert .= "'".parse_data($address->{Katuosoite})." ".parse_data($address->{Postinumero})." ".parse_data($address->{Postitoimipaikka})."',";
        $insert .= "'EUR',";
        $insert .= "'EUR',";
        $insert .= "'".parse_data($source->{Asiakasnumero})."'";
        $insert .= ")";

        #print "Bookseller: $insert\n";

        $dbh->do($insert);
    }

}

sub get_biblionumber {
    my ($id) = @_;
    $id =~ s/\x00//g;
    my $query = "SELECT biblionumber, biblioitemnumber FROM ".$destinationdb.".biblioitems where collectiontitle = ?;";
    my $sth=$dbh->prepare($query);
    $sth->execute($id);

    return $sth->fetchrow_hashref;

}

sub get_address {
    my ($id, $order) = @_;
    $id =~ s/\x00//g;
    $order =~ s/\x00//g;
    my $query = "SELECT Katuosoite, Postitoimipaikka, Postinumero FROM ".$sourcedb.".osoitteet where KontaktiID = ? and REPLACE(Jarjestys,'\r','') = ?";
    my $sth=$dbh->prepare($query);
    $sth->execute($id, $order);

    return $sth->fetchrow_hashref;

}


sub set_itemcallnumber {
    my ($luokka, $paasana, $piste, $osasto) = @_;
    my $itemcallnumber;
    my $new_piste = substr($piste, 4, index($piste, '_'));
    my $new_paasana = substr($paasana, 0, 3);
    $itemcallnumber = $luokka." ".$new_paasana." ".$new_piste." ".$osasto;
    return $itemcallnumber;
}


sub parse_data {
    my ($data) = @_;
    $data =~ s/\x00//g;
    return trim($data);
}

sub set_boolean {
    my ($data) = @_;
    if ($data eq "True") {
        $data = "1";
    } else {
        $data = "0";
    }

    return $data;
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

sub location_change {
    my ($location) = @_;

    $location = parse_data($location);
    my $query = "SELECT lyhenne FROM ".$sourcedb.".osastot WHERE Id = ?";
    my $sth=$dbh->prepare($query);
    $sth->execute($location);

    return $sth->fetchrow;
}

sub ccode_change {
    my ($ccode) = @_;
    $ccode = parse_data($ccode);
    my $query = "SELECT lyhenne FROM ".$sourcedb.".hyllyt WHERE Id = ?";
    my $sth=$dbh->prepare($query);
    $sth->execute($ccode);

    return $sth->fetchrow;
}

sub subloc_change {
    my ($type) = @_;
    my $new_type;
    if ($type eq "10") {
        $new_type = "AUV";
    }
    if ($type eq "11") {
        $new_type = "ALA";
    }
    if ($type eq "13") {
        $new_type = "ALL";
    }
    if ($type eq "14") {
        $new_type = "KUV";
    }
    if ($type eq "17") {
        $new_type = "VUV";
    }
    return $new_type;
}

sub notforloan_change {
    my ($type) = @_;
    $type = parse_data($type);
    my $new_type;
    my $desc;
    if ($type eq "1") {
        $new_type = "2";
    }
    if ($type eq "2") {
        $new_type = "4";
    }
    if ($type eq "4") {
        $new_type = "3";
    }
    if ($type eq "6") {
        $new_type = "-3";
    }
    if ($type eq "19") {
        $new_type = "-2";
    }

    return $new_type;
}

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
    insert_itypes($new_type, encode_to($desc));
    return $new_type;
}

sub insert_itypes {
    my ($type, $desc) = @_;
    my $query = "SELECT itemtype FROM ".$destinationdb.".itemtypes WHERE itemtype = ?";
    my $sth=$dbh->prepare($query);
    $sth->execute($type);
    unless (my $row = $sth->fetchrow) {
        my $insert = "INSERT INTO ".$destinationdb.".itemtypes 
        (itemtype,
        description) VALUES (?,?)";
        my $sth1=$dbh->prepare($insert);
        $sth1->execute($type, $desc);

    }
    
}

sub encode_to {
    my ( $value ) = @_;
    return encode( 'iso-8859-1', $value);
}