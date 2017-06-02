<?php
require_once 'config.php';
$file = "data/american-election-tweets.csv";

//Daten zum Speichern vorbereiten
$data = [];
$i = 0;
//Versuche die Datei zu öffnen um nötige Daten zu extrahieren
$fh = fopen($file, "r") or die("cannot read file ".$file);
while(($row = fgetcsv($fh,1000,';')) !== false){
    //erste Zeite weglassen
    if($i > 0){
        //Daten zusammensetzen
        $tweet = [
            'autor'     => $row[0],
            'message'   => $row[1],
            'created'   => str_replace("T"," ", $row[4]),
            'retweets'  => $row[7],
            'favorites' => $row[8]
        ];
        //Hashtags extrahieren
        preg_match_all("/#(\\w+)/", $row[1], $matches);
        $tweet['tags'] = $matches[1] ?? [];
        /**
         * TODO: Hashtagspaare extrahieren
         * 
         * Versucht: preg_match_all("/#(\\w+)\s?#(\\w+)/im", $row[1], $matches2,PREG_OFFSET_CAPTURE);
         */
        
        $data[] = $tweet;
    }
    $i++;
}

//Nachdem die Daten vorbereitet wurden, werden sie in der Datenbank import
#Datenbankverbindung erstellen
try{
    $conn = new PDO("pgsql:dbname=".SQL_DB.";host=".SQL_HOST, SQL_USER, SQL_PASS);
} catch (Exception $ex) {
    echo $ex->getTraceAsString();
}


if($data){//Überprüft ob Daten überhaupt vorhanden sind
    foreach ($data as $k){       
        //Tweets speichern
        $qt = "INSERT INTO tweets(autor,message) VALUES(:a,:m)";
        $stmnt = $conn->prepare($qt);
        $stmnt->bindValue(':a',$k['autor'],PDO::PARAM_STR);
        $stmnt->bindValue(':m', trim($k['message']), PDO::PARAM_STR);
        //$stmnt->bindValue(':c', trim($k['created']));
        $stmnt->execute();
        
        $tweeID = $conn->lastInsertId('tweets_ID_seq');
        
        //Falls der tweet erfolgreich gespeichert werden konnte, popularity und hashtags speichern
        if($tweeID){
            $qp = "INSERT INTO popularity SET tweetID=:tid,favorites=:f,retweets=:rt";
            $stmnt = $conn->prepare($qp);
            $stmnt->execute([':tid'=>$tweeID,':f'=>$k['favorites'],':rt'=>$k['favovites']]);
            
            //Tags behandeln falls vorhanden
            if($k['tags']){
                foreach ($k['tags'] as $t => $v){
                    //Tag speichern und tagID holen
                    $qtag = "INSERT INTO hashtags SET tagName=:tn ON DUPLICATE KEY UPDATE ID=LAST_INSERT_ID(ID)";
                    $stmnt = $conn->prepare($qtag);
                    $stmnt->execute([':tn'=> trim($v)]);
                    $tagID = $conn->lastInsertId('hashtags_ID_seq');
                    if($tagID){
                        $stmnt = $conn->prepare("INSERT INTO hathashtags SET tweetID=:tid,tagID=:tagID ON DUPLICATE KEY UPDATE popularity=popularity+1");
                        $stmnt->execute([':tid'=>$tweeID,':tagID'=>$tagID]);
                    }
                }
            }
        }
    }
}