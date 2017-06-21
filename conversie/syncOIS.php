<?php
require_once 'simplexlsx.class.php';
require_once "settings.php";
include("../../CKAN/conversie/ckan.php");
      
error_reporting(E_ALL ^E_NOTICE ^E_WARNING);
set_time_limit(600);
ini_set('memory_limit', '256M');

class SyncOIS{
    var $url;
    var $xslx;
    var $onderwerpen;
    var $bestanden;
    var $groupmapping;
    var $ckan;
        
    function SyncOIS($url){
        $this->groupmapping =  Array(
            "Kerncijfers" => "bevolking",
            "Bevolking" => "bevolking",
            "Openbare orde en veiligheid" => "openbare-orde-veiligheid",
            "Werk en inkomen" => "werk-inkomen",
            "Zorg" => "zorg-welzijn",
            "Gezondheid" => "zorg-welzijn",
            "Onderwijs" => "educatie-jeugd-diversiteit",
            "Verkeer en infrastructuur" => "verkeer-infrastructuur",
            "Openbare ruimte en groen" => "openbare-ruimte-groen",
            "Cultuur en monumenten" => "toerisme-cultuur",
            "Milieu en water" => "milieu-water",
            "Sport en recreatie" => "sport-recreatie",
            "Economie en haven" => "economie-haven",
            "Stedelijke ontwikkeling" => "stedelijke-ontwikkeling",
            "Middelen" => "bestuur-en-organisatie",
            "Bestuur en concern" => "bestuur-en-organisatie",
            "Tijdreeksen" => "bevolking",
            "Prognoses" => "bevolking",
            "Educatie" => "educatie-jeugd-diversiteit",
            "Milieu" => "milieu-water",
            "Verkiezingen" => "verkiezingen",
            "Veiligheid" => "openbare-orde-veiligheid",
            "Economie" => "economie-haven",
            "Inkomen en sociale zekerheid" => "werk-inkomen",
            "Gezondheid en welzijn" => "zorg-welzijn",
            "Verkeer en vervoer" => "verkeer-infrastructuur",
            "Natuur en milieu" => "milieu-water",
            "Toerisme" => "toerisme-cultuur",
            "Bouwen en wonen" => "wonen-leefomgeving",
            "Politiek" => "verkiezingen",
            "Bestuur" => "bestuur-en-organisatie",
            "Openbare ruimte" => "openbare-ruimte-groen",
            "Kaart" => "geografie"
        );
        
        $this->ckan = new CKAN();
        $this->download($url);
        $this->readSource();
        $this->sync();
    }
    
    /*
    Groups:
    "bestuur-en-organisatie",
    "bevolking",
    "dienstverlening",
    "economie-haven",
    "educatie-jeugd-diversiteit",
    "energie",
    "geografie",
    "milieu-water",
    "openbare-orde-veiligheid",
    "openbare-ruimte-groen",
    "sport-recreatie",
    "stedelijke-ontwikkeling",
    "toerisme-cultuur",
    "verkeer-infrastructuur",
    "verkiezingen",
    "werk-inkomen",
    "wonen-leefomgeving",
    "zorg-welzijn"
    */
    
    function download($url){
        $xlsx = file_get_contents($url);
        $f = fopen("../data/meta.xslx","w");
        fwrite($f, $xlsx);
        fclose($f);
        $this->url = "../data/meta.xslx";
    }
    
    function readSource(){
        $this->xlsx = new SimpleXLSX($this->url);
    }
    
    function sync(){
        $onderwerpen = Array();
        
        foreach($this->xlsx->sheetNames() as $key => $name){
            if($name == "onderwerpen") $onderwerpen = $this->xlsx->rows($key);
            if($name == "bestanden") $this->bestanden = $this->xlsx->rows($key);
        }
        
        //Alle onderwerpen met daarin 'Feiten en cijfers >' toevoegen aan lijst met onderwerpen
        $tempArr = Array();
        foreach($onderwerpen as $key => $onderwerp){
            if(stripos($onderwerp[1],"Feiten en cijfers >") !== false){
                $this->onderwerpen[] = $onderwerp;
                $tempArr[] = $onderwerp[1];
            }
        }
        //Filter op onderwerpen die 'subonderwerpen' hebben
        $tempArr2 = Array();
        foreach($this->onderwerpen as $key => $onderwerp){
            $parent_onderwerp = false;
            foreach($tempArr as $titel){
                if(stripos($titel, $onderwerp[1]) !== false && strlen($titel) > strlen($onderwerp[1])){
                    //$parent_onderwerp = true;
                    //Skip parent check. Niet meer nodig.
                } else {
                }
            }
            if(!$parent_onderwerp) $tempArr2[] = $onderwerp;
        }
        $this->onderwerpen = $tempArr2;

        $this->ckan->getDatasets();
        
        foreach($this->onderwerpen as $onderwerp){
            $this->syncOnderwerp($onderwerp);
        }
        
        $this->collectGarbage();
    }
    
    function syncOnderwerp($onderwerp){
        $id = $onderwerp[0];
        $name = "ois-" . $onderwerp[0];
        
        if($this->ckan->setExists($name)){
            $this->updateDataset($name, $onderwerp);
        } else {
            $this->createDataset($name, $onderwerp);
        }
    }

    function updateDataset($name, $onderwerp){
        $set = $this->getSet($name, $onderwerp);

        if($set){
            //Check of er een update nodig is!
            $match = true;
            
            //Titel (onderwerp kan veranderd zijn)
            list($fenc, $gebied, $thema, $subthema) = explode(" > ",$onderwerp[1]);
            if($gebied == "Feiten en cijfers") $gebied = $fenc;
            $titel = $thema;
            if(trim($subthema) <> "") $titel .= " - ". $subthema;
            $titel .= " (". $gebied .")"; 
            if($titel != $set["title"]) $match = false;

            //Links
            $current_resources = $this->ckan->datasets[$name]->res_url;
            foreach($set["resources"] as $res_url){
                if(in_array($res_url["url"], $current_resources)){
                } else {
                    $match = false;
                }
            }
            
            if(!$match || count($current_resources) <> count($set["resources"])){
                $set["metadata_modified"] = date("Y-m-d\TH:i:s");
                $this->ckan->setSet($set);
            } else {
            }
        }
    }
    
    function createDataset($name, $onderwerp){
        $set = $this->getSet($name, $onderwerp);
        if($set){
            $set["metadata_created"] = date("Y-m-d\TH:i:s");
            $this->ckan->createDataset($set);
        }
    }
    
    function getSet($name, $onderwerp){
        $id = $onderwerp[0];
        $resources = Array();
        $tags = Array("ois");
        
        list($fenc, $gebied, $thema, $subthema) = explode(" > ",$onderwerp[1]);
        if($gebied == "Feiten en cijfers") $gebied = $fenc;
        
        if(trim($thema) == "") $thema = "Feiten en cijfers";
        $titel = $thema;
        if(trim($subthema) <> "") $titel .= " - ". $subthema;
        $titel .= " (". $gebied .")";  

        $notes = "<p>Diverse datasets met statistieken van Onderzoek, Informatie en Statistiek.</p><p>Thema: ". $thema;
        if(trim($subthema) <> "") $notes .= ", <br/>Onderwerp:  ". $subthema;
        if(trim($gebied) <> "") $notes .= ", <br/>Detailniveau: ". $gebied ."</p>";
                             
                             
        if(!$this->groupmapping[$thema]){
            $group = Array(["name" => "Bevolking"]);
        } else {
            $group = Array(["name" => $this->groupmapping[$thema]]);            
        }
        
        foreach($this->bestanden as $bestand){
            if($bestand[3] == $id && in_array($bestand[1], ["xlsx","xls","zip"])){
                $url = str_replace(Array(".",",","/","`","'","(",")","%",":","+"),"", strtolower(trim($bestand[2])));
                $url = str_replace(Array("        ","       ","      ","    ","   ","  "," "),"-", $url);
                $url = "http://www.ois.amsterdam.nl/download/" . $url .".xlsx";
                $resources[] = Array("description" => "", "format" => "XLS", "name" => $bestand[2], "url" =>$url);
            
                if(!in_array($bestand[8],$tags)){
                    foreach(explode(",", $bestand[8]) as $tag){
                        $tags[] = trim(str_replace(["\\","/","&"],"-",$tag));
                    }
                }
            }
        }
        
        if(count($resources) > 0){
            $tagArr = Array();
            foreach($tags as $tag){
                $tagArr[] = Array("name" => $tag);
            }
            
            $set = Array();
            
            $set["name"] = $name;
            $set["title"] = $titel;
            $set["author"] = "Gemeente Amsterdam, Onderzoek, Informatie en Statistiek";
            $set["author_email"] = "algemeen.OIS@amsterdam.nl";
            $set["maintainer"] = "Gemeente Amsterdam, Onderzoek, Informatie en Statistiek";
            $set["maintainer_email"] = "algemeen.OIS@amsterdam.nl";
            $set["license_id"] = "cc-by";
            $set["notes"] = $notes;
            $set["type"] = "dataset";
            $set["resources"] = $resources;
            $set["tags"] = $tagArr;
            $set["private"] = "false";
            $set["state"] = "active";
            $set["owner_org"] = "b7ddef0f-f7c9-466f-9e63-a28aaf520024";
            $set["groups"] = $group;

            return $set;
        }
        
    }

    function collectGarbage(){
        foreach($this->ckan->datasets as $set){
            if(substr($set->name,0,4) == "ois-"){
                $set_onderwerp = substr($set->name,4);
                $found = false;
                foreach($this->onderwerpen as $onderwerp){
                    if($onderwerp[0] == $set_onderwerp) $found = true;
                }
                if(!$found){
                    print("\n<BR>Onderwerp <a href='https://api.datapunt.amsterdam.nl/catalogus/dataset/". $set->name ."' target='_blank'>". $set->name ."</a> bestaat niet meer. ");
                }
            }
        }
    }   
    
}

/*


Parameters:    
name (string) – the name of the new dataset, must be between 2 and 100 characters long and contain only lowercase alphanumeric characters, - and _, e.g. 'warandpeace'
title (string) – the title of the dataset (optional, default: same as name)
author (string) – the name of the dataset’s author (optional)
author_email (string) – the email address of the dataset’s author (optional)
maintainer (string) – the name of the dataset’s maintainer (optional)
maintainer_email (string) – the email address of the dataset’s maintainer (optional)
license_id (license id string) – the id of the dataset’s license, see license_list() for available values (optional)
notes (string) – a description of the dataset (optional)
url (string) – a URL for the dataset’s source (optional)
version (string, no longer than 100 characters) – (optional)
state (string) – the current state of the dataset, e.g. 'active' or 'deleted', only active datasets show up in search results and other lists of datasets, this parameter will be ignored if you are not authorized to change the state of the dataset (optional, default: 'active')
type (string) – the type of the dataset (optional), IDatasetForm plugins associate themselves with different dataset types and provide custom dataset handling behaviour for these types
resources (list of resource dictionaries) – the dataset’s resources, see resource_create() for the format of resource dictionaries (optional)
tags (list of tag dictionaries) – the dataset’s tags, see tag_create() for the format of tag dictionaries (optional)
extras (list of dataset extra dictionaries) – the dataset’s extras (optional), extras are arbitrary (key: value) metadata items that can be added to datasets, each extra dictionary should have keys 'key' (a string), 'value' (a string)
relationships_as_object (list of relationship dictionaries) – see package_relationship_create() for the format of relationship dictionaries (optional)
relationships_as_subject (list of relationship dictionaries) – see package_relationship_create() for the format of relationship dictionaries (optional)
groups (list of dictionaries) – the groups to which the dataset belongs (optional), each group dictionary should have one or more of the following keys which identify an existing group: 'id' (the id of the group, string), or 'name' (the name of the group, string), to see which groups exist call group_list()
owner_org (string) – the id of the dataset’s owning organization, see organization_list() or organization_list_for_user() for available values (optional)


http://data.amsterdam.nl/api/3/action/package_show?id=basisbestand-gebieden-amsterdam--bbga-

{
help: "http://data.amsterdam.nl/api/3/action/help_show?name=package_show",
success: true,
result: {
owner_org: "b7ddef0f-f7c9-466f-9e63-a28aaf520024",
maintainer: "Lieselotte Bicknese",
relationships_as_object: [ ],
private: false,
maintainer_email: "l.bicknese@amsterdam.nl",
num_tags: 4,
update_frequency: "Anders",
odi-certificate: "",
id: "266e09ec-8ef5-4a6a-8e69-2dbc832d03ef",
metadata_created: "2016-02-25T13:52:51",
metadata_modified: "2016-06-15T12:10:36",
author: "Gemeente Amsterdam, Onderzoek, Innovatie & Statistiek.nl",
author_email: "algemeen.OIS@amsterdam.nl",
state: "active",
notes_markdown: "Het Basisbestand Gebieden Amsterdam (BBGA) bevat kerncijfers voor meerdere gebiedsindelingen (stadsdelen, buurtcombinaties, buurten, etc.) in Amsterdam. De kerncijfers bestaan uit meer dan 400 variabelen over diverse thema's: * Bevolking * Leeftijd * Wonen * Openbare ruimte * Verkeer * Leefbaarheid * Veiligheid * Bedrijvigheid * Sport en recreatie * Welzijn en zorg * Onderwijs * Inkomen * Participatie ",
email_notify_maintainer: false,
version: null,
license_id: "cc-by",
type: "dataset",
license_text: "",
resources: [
{
cache_last_updated: "2016-02-25T13:06:13.156522",
package_id: "266e09ec-8ef5-4a6a-8e69-2dbc832d03ef",
csvlint_json: "{}",
webstore_last_updated: null,
id: "6706ae42-1f15-4e43-b0d4-c9f3f525620e",
size: "22901",
state: "active",
datastore_json: "{}",
hash: "",
description: "via link op de website",
format: "XLS",
mimetype_inner: null,
url_type: "200",
mimetype: "text/html; charset=utf-8",
cache_url: null,
name: "Basisbestand Gebieden Amsterdam",
created: "2016-02-25T13:06:12.611391",
url: "http://www.ois.amsterdam.nl/online-producten/basisbestand-gebieden-amsterdam",
webstore_url: null,
lazyboy_json: "{}",
last_modified: null,
position: 0,
revision_id: "bd8d5285-1925-4a83-ab6e-afd4d2f35f6c",
resource_type: null
},
{
cache_last_updated: "2016-02-25T13:07:06.409827",
package_id: "266e09ec-8ef5-4a6a-8e69-2dbc832d03ef",
csvlint_json: "{}",
webstore_last_updated: null,
id: "363a72a3-ffcd-41c5-8562-3d35470b5071",
size: "22136",
state: "active",
datastore_json: "{}",
hash: "",
description: "Voorbeeld visualisatie van BBGA",
format: "HTML",
mimetype_inner: null,
url_type: "200",
mimetype: "text/html; charset=utf-8",
cache_url: null,
name: "Dashboard Kerncijfers",
created: "2016-02-25T13:07:06.024760",
url: "http://www.ois.amsterdam.nl/visualisatie/dashboard_kerncijfers.html",
webstore_url: null,
lazyboy_json: "{}",
last_modified: null,
position: 1,
revision_id: "bd8d5285-1925-4a83-ab6e-afd4d2f35f6c",
resource_type: null
}
],
website: "http://www.ois.amsterdam.nl/",
num_resources: 2,
email_notify_author: false,
tags: [
{
vocabulary_id: null,
state: "active",
display_name: "demografie",
id: "fc61c16a-a6a1-4912-a8f6-9f1b5939e496",
name: "demografie"
},
{
vocabulary_id: null,
state: "active",
display_name: "gebieden",
id: "fd68b5bb-5ecc-4f0a-939e-c869c8f718b6",
name: "gebieden"
},
{
vocabulary_id: null,
state: "active",
display_name: "kerncijfers",
id: "f3c13897-ab0b-4861-a29e-6f969e1c7781",
name: "kerncijfers"
},
{
vocabulary_id: null,
state: "active",
display_name: "statistieken",
id: "b542a2f6-1ec1-4e1d-9640-6cbeff0a3749",
name: "statistieken"
}
],
title: "Basisbestand Gebieden Amsterdam (BBGA)",
featured_dataset: true,
groups: [
{
display_name: "Bevolking",
description: "",
title: "Bevolking",
image_display_url: "/wp-content/themes/theme_amsterdam/img/topic/bevolking.png",
id: "6bccb292-98ad-40f9-9407-4653bf70b86c",
name: "bevolking"
}
],
creator_user_id: "7f28293e-41e4-43f1-8ff0-0f5bc7f30998",
relationships_as_subject: [ ],
dp_modified: "2016-06-15T12:10:36",
name: "basisbestand-gebieden-amsterdam--bbga-",
isopen: true,
url: null,
notes: "<p><span style="color: #333333; font-family: verdana, arial, sans-serif; font-size: 10.9994px; line-height: 16.4991px;">Het Basisbestand Gebieden Amsterdam (BBGA) bevat kerncijfers </span><span style="color: #333333; font-family: verdana, arial, sans-serif; font-size: 10.9994px; line-height: 16.4991px;"> voor meerdere gebiedsindelingen (stadsdelen, buurtcombinaties, buurten, etc.) in Amsterdam.</span></p> <p><span style="color: #333333; font-family: verdana, arial, sans-serif; font-size: 10.9994px; line-height: 16.4991px;">De kerncijfers bestaan uit meer dan 400 variabelen over diverse thema's:</span></p> <ul> <li><span style="color: #333333; font-family: verdana, arial, sans-serif;"><span style="font-size: 10.9994px; line-height: 16.4991px;">Bevolking</span></span></li> <li><span style="color: #333333; font-family: verdana, arial, sans-serif;"><span style="font-size: 10.9994px; line-height: 16.4991px;">Leeftijd</span></span></li> <li><span style="color: #333333; font-family: verdana, arial, sans-serif;"><span style="font-size: 10.9994px; line-height: 16.4991px;">Wonen</span></span></li> <li><span style="color: #333333; font-family: verdana, arial, sans-serif;"><span style="font-size: 10.9994px; line-height: 16.4991px;">Openbare ruimte</span></span></li> <li><span style="color: #333333; font-family: verdana, arial, sans-serif;"><span style="font-size: 10.9994px; line-height: 16.4991px;">Verkeer</span></span></li> <li><span style="color: #333333; font-family: verdana, arial, sans-serif;"><span style="font-size: 10.9994px; line-height: 16.4991px;">Leefbaarheid</span></span></li> <li><span style="color: #333333; font-family: verdana, arial, sans-serif;"><span style="font-size: 10.9994px; line-height: 16.4991px;">Veiligheid</span></span></li> <li><span style="color: #333333; font-family: verdana, arial, sans-serif;"><span style="font-size: 10.9994px; line-height: 16.4991px;">Bedrijvigheid</span></span></li> <li>Sport en recreatie</li> <li><span style="color: #333333; font-family: verdana, arial, sans-serif;"><span style="font-size: 10.9994px; line-height: 16.4991px;">Welzijn en zorg</span></span></li> <li><span style="color: #333333; font-family: verdana, arial, sans-serif;"><span style="font-size: 10.9994px; line-height: 16.4991px;">Onderwijs</span></span></li> <li><span style="color: #333333; font-family: verdana, arial, sans-serif;"><span style="font-size: 10.9994px; line-height: 16.4991px;">Inkomen</span></span></li> <li><span style="color: #333333; font-family: verdana, arial, sans-serif;"><span style="font-size: 10.9994px; line-height: 16.4991px;">Participatie</span></span></li> </ul> <p>&nbsp;</p>",
license_title: "Creative Commons Attribution",
license_url: "http://www.opendefinition.org/licenses/cc-by",
organization: {
description: "<a href='http://www.ois.amsterdam.nl'>www.ois.amsterdam.nl</a>",
title: "Gemeente Amsterdam, Onderzoek, Informatie en Statistiek",
created: "2016-01-20T13:38:33.723588",
approval_status: "approved",
is_organization: true,
state: "active",
image_url: "https://files.datapress.com/amsterdam/wp-uploads/20160216153028/amsterdam.png",
revision_id: "e80a4098-4b36-4eee-9c27-98789eb0516a",
type: "organization",
id: "b7ddef0f-f7c9-466f-9e63-a28aaf520024",
name: "gemeente-amsterdam-onderzoek-informatie-en-statistiek"
},
revision_id: "47214d03-b98e-4fd8-a811-1c4084e407f3",
dp_created: "2016-02-25T13:52:51"
}
}

*/
  
  
$sync = new SyncOIS("http://open.data.amsterdam.nl/OIS/meta.xlsx");
?>