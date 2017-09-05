#!/usr/bin/perl

use strict;
use warnings; 

use XML::LibXML;
use XML::LibXML::PrettyPrint;
use XML::XPath;
use Data::Dumper;

my $file = 'OrigoMigration/MARC/MARC21.xml';

my $count = 0;
my $added = 0;
my $total = 0;
open (my $fh, '<',$file)  or die "cant open the file: $! \n";
#my @marcxmls = split(/(?=<record format="MARC21" type="Bibliographic">)/,<$fh>);
my($xp) = XML::XPath->new( xml => join('', <$fh>) );
my($nodeset) = $xp->find( '/record' );

foreach my $record ( $nodeset->get_nodelist() ) {
	print $record->toString();
}

#foreach my $record (@marcxmls) {
	#print Dumper $record;
	# my $tag_addr = $node->getAttribute('tag');
	# my $parentnode = $node->parentNode;
	# $total++;
	# $added = 0;
	# if ($tag_addr eq "245") {
	# 	$added = 1;
	# 	$parentnode->unbindNode;
	# }
	# if ($tag_addr eq "240") {
	# 	$added = 1;
	# 	$parentnode->unbindNode;
	# }
	# if ($tag_addr eq "100") {
	# 	$added = 1;
	# 	$parentnode->unbindNode;
	# }
	# if ($tag_addr eq "110") {
	# 	$added = 1;
	# 	$parentnode->unbindNode;
	# }
	# if ($added) {
	# 	$count++;
	# 	print "$count\n";

	# }
#}

print "Total: $total\n";
#my $pp = XML::LibXML::PrettyPrint->new;
#$pp->pretty_print($doc)->toFile('parsed_xml.xml');