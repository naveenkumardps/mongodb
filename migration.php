<?php
$path = dirname(__FILE__);
require_once $path . '/../../../init.php';
require_once $path . '../../../../lib/vendor/autoload.php';
//DAC connect DB
if ($GLOBALS['_MAX']['CONF']['database']['type'] == 'mysql') {
    require_once MAX_PATH . '/lib/OA/Dal/Delivery/mysql.php';
}elseif ($GLOBALS['_MAX']['CONF']['database']['type'] == 'pgsql') {
    require_once MAX_PATH . '/lib/OA/Dal/Delivery/pgsql.php';
}elseif ($GLOBALS['_MAX']['CONF']['database']['type'] == 'mysqli') {
     require_once MAX_PATH . '/lib/OA/Dal/Delivery/mysqli.php';
}
 $table_prefix = $GLOBALS['_MAX']['CONF']['table']['prefix'];
$select=OA_Dal_Delivery_query("SELECT * FROM {$table_prefix}mongodbsetting") or die("error in db connect");
$row=OA_Dal_Delivery_fetchAssoc($select);
error_reporting(E_ALL);
ini_set('display_errors', 1);
define('MONGOHOST',$row['mongohost']);
define('MONGOUSER',$row['mongouser']);
define('MONGOPASS',$row['mongopass']);
define('MONGODB',$row['mongodb']);
try{
$manager = new MongoDB\Driver\Manager("mongodb://".MONGOUSER.":".MONGOPASS."@".MONGOHOST.":27017/".MONGODB);
}catch(Exception $e){
 var_dump($e);
}
$table_prefix = $GLOBALS['_MAX']['CONF']['table']['prefix'];
$get_timezone = OA_Dal_Delivery_fetchAssoc(OA_Dal_Delivery_query("SELECT value FROM {$table_prefix}account_preference_assoc WHERE preference_id='16'")) or die("Get time zone");
$c = new DateTime($get_timezone['value']);
/*if you are manually running cron for a set of period then you can pass parameter of server date in following format
/cron.php?start={Y-m-d H:i:s}&end={Y-m-d H:i:s}
*/
if(isset($_GET["start"]) && isset($_GET["end"])) {
	$start =$_GET["start"];
	$end = $_GET["end"];
}
/*end of dynamic update*/
else{
$hourly_date_time = date('Y-m-d H:00:00');
$newdate = date('Y-m-d H:m:s',$d->sec);
$start = date("Y-m-d H:00:00",strtotime('-1 hour',strtotime(date('Y-m-d H:i:s'))));
$end = date("Y-m-d H:59:59",strtotime('-1 hour',strtotime(date('Y-m-d H:i:s'))));
}
//request insertion start		
	$command = new MongoDB\Driver\Command([
    'aggregate' => 'ads_request',
    'pipeline' => [
		[
			'$match'=>[
				'$and'=>[
					["date_time"=>[
						'$gte'=>'2019-10-01 04:00:00'
						]
					],
					["date_time"=>[
						'$lte'=>'2019-10-01 04:59:00'
						]
					]
				]

			]
		],
        [
        '$group' => [
			"_id" => [
				"country_name" => '$country_name',
				"ip" => '$ip',
				"browser_name" => '$browser.name',
				"os_name" => '$os.name',
				"device_name" => '$device.vendor',
				"device_model" => '$device.model',
				"device_type" => '$device.type',
				"country_code" => '$country_code',
				"city" => '$city',
				"region_name" => '$region_name',
				"ua" => '$ua',
				"type" => '$type',				
			],
			"count" =>[
					'$sum' => 1
				]
			]
       ],
    ],

    'cursor' => new stdClass,
]);
//$del = array('datetime'=>array('$gte'=>$start,'$lte'=>$end));
$reqbids = $manager->executeCommand(MONGODB, $command);
$requestbids = array();

foreach ($reqbids as $document) {
	
   $requestbids[] = (array)$document;
}

if(is_array($requestbids)&& !empty($requestbids)){
$sql = "INSERT INTO `rv_dj_ba_statss`( `country`, `country_name`, `os`, `ip`, `user_agent`, `device_type`, `device_make`, `device_model`, `request_count`,mediatype , `datatime`) VALUES ";
    $reqobj = array();//$i=0;
	for($i=0;$i<count($requestbids);$i++)
	{
//checking value to table
		$sdate=date('Y-m-d 00:00:00');
		$edate=date('Y-m-d 23:59:59');
		$country = get_spd_value($requestbids[$i]['_id']->country_code,'country');
		$os = get_spd_value($requestbids[$i]['_id']->os_name,'os');
		//$data['domain'] =  get_spd_value($row['domain'],'domain');
		$ip =  get_spd_value($requestbids[$i]['_id']->ip,'ip');
		$user_agent =  get_spd_value($requestbids[$i]['_id']->ua,'user_agent');
		$device_type = get_spd_value($requestbids[$i]['_id']->device_type,'device_type');
		$device_make = get_spd_value($requestbids[$i]['_id']->device_name,'device_make');
		$device_model = get_spd_value($requestbids[$i]['_id']->device_model,'device_model');
		$count=$requestbids[$i]['count'];
		$datatime= date('Y-m-d H:00:00');
		$mediatype = $requestbids[$i]['_id']->type;
		 $query11=OA_Dal_Delivery_query("SELECT id,request_count FROM rv_dj_ba_statss WHERE  country='".$country."' AND country_name='".$country."' AND ip='".$ip."' AND user_agent='".$user_agent."' AND device_type='".$device_type."' 
		AND device_model='".$device_model."'  AND mediatype='".$mediatype."' AND datatime BETWEEN '".$sdate."' AND '".$edate."'");
		$DATA=OA_Dal_Delivery_fetchAssoc($query11);
		if($DATA['id'])
			{
			 $tot_count=$DATA['request_count']+$count;
			 OA_Dal_Delivery_query("UPDATE `rv_dj_ba_statss` SET request_count='".$tot_count."' WHERE id='".$DATA['id']."'");
			}
		$reqobj[] = "($country,$country,$os,$ip,$user_agent,$device_type,$device_make,$device_model,$count,'$mediatype','$start')";
			}
			$sql .= implode(',', $reqobj);
			if(!$DATA['id'])
			{OA_Dal_Delivery_query($sql) or die(mysql_error());}
			
  //to delete the selected row form MOngodb database DAC140 
	//  $del = array('date_time'=>array('$gte'=>$start,'$lte'=>$end));
	// $reqdelte = new MongoDB\Driver\BulkWrite;
	// $reqdelte->delete($del);
	// $dels=$manager->executeBulkWrite(MONGODB.'.ads_request', $reqdelte);

}
//request insertion end
//response insertion start
$rescommand = new MongoDB\Driver\Command([
    'aggregate' => 'ad_response',
    'pipeline' => [
		[
			'$match'=>[
					'$and'=>[
					["date_time"=>[
						'$gte'=>'2019-10-01 04:00:00'
						]
					],
					["date_time"=>[
						'$lte'=>'2019-10-01 04:59:00'
						]
					]
				]
			]	
		],
        [
        '$group' => [
			"_id" => [
				"country_name" => '$country_name',
				"creativeId" => '$creativeId',
				"bannerId" => '$bannerId',
				"ip" => '$ip',
				"browser_name" => '$browser.name',
				"os_name" => '$os.name',
				"device_name" => '$device.vendor',
				"device_model" => '$device.model',
				"device_type" => '$device.type',
				"country_code" => '$country_code',
				"city" => '$city',
				"region_name" => '$region_name',
				"ua" => '$ua',
				"mediaType" => '$mediaType',	
				],
				"count" => ['$sum' => 1],
				
		]
       ],
    ],
    'cursor' => new stdClass,
]);
$resbids = $manager->executeCommand(MONGODB, $rescommand);
$responsebids = array();

	
foreach ($resbids as $resdocument) {
   $responsebids[] = (array)$resdocument;
}

if(is_array($responsebids) && !empty($responsebids)){

    $resobj = array();//foreach($resbids as $res)
	for($j=0;$j<count($responsebids);$j++)
	{
		///print_r($responsebids);
		$country = get_spd_value($requestbids[$j]['_id']->country_code,'country');
		//print_r($country);
		$os = get_spd_value($requestbids[$j]['_id']->os_name,'os');
		//$data['domain'] =  get_spd_value($row['domain'],'domain');
		$ip =  get_spd_value($responsebids[$j]['_id']->ip,'ip');
		$user_agent =  get_spd_value($responsebids[$j]['_id']->ua,'user_agent');
		$device_type = get_spd_value($responsebids[$j]['_id']->device_type,'device_type');
		$device_make = get_spd_value($responsebids[$j]['_id']->device_name,'device_make');
		$device_model = get_spd_value($responsebids[$j]['_id']->device_model,'device_model');
		$creativeId = $responsebids[$j]['_id']->creativeId;
		$bannerId = $responsebids[$j]['_id']->bannerId;
		$count=$responsebids[$j]['count'];
		$datatime= date('Y-m-d H:00:00');
		$mediatype = $responsebids[$j]['_id']->mediaType;


		$query12=OA_Dal_Delivery_query("SELECT * FROM rv_dj_ba_statss WHERE  country='".$country."' AND country_name='".$country."' AND ip='".$ip."' AND user_agent='".$user_agent."' AND device_type='".$device_type."' 
		AND device_model='".$device_model."'  AND mediatype='".$mediatype."' AND campaignid='".$creativeId."' AND bannerid='".$bannerId."' AND datatime BETWEEN '".$sdate."' AND '".$edate."'");
		
		
		$DATA1=OA_Dal_Delivery_fetchAssoc($query12);
			print_r($DATA1);

			// if ($DATA1['campaignid']==0 && $DATA1['bannerid']==0) {

		 // 	OA_Dal_Delivery_query("UPDATE `rv_dj_ba_statss` SET campaignid='".$creativeId."',bannerid='".$bannerId."' WHERE id='".$DATA1['id']."'");
		 // }
		 // if($DATA1['campaignid']==$creativeId && $DATA1['bannerid']==$bannerId)
			// {
 		// 		$tot_count1=$DATA1['response_count']+$count;
			//     OA_Dal_Delivery_query("UPDATE `rv_dj_ba_statss` SET response_count='".$tot_count1."' WHERE id='".$DATA1['id']."' and campaignid='".$creativeId."' and bannerid='".$bannerId."'");
				
			// }
			// else{

			// 		$sql1="INSERT INTO `rv_dj_ba_statss`(`campaignid`,`bannerid`, `country`, `country_name`, `os`, `ip`, `user_agent`, `device_type`, `device_make`, `device_model`, `response_count`,mediatype , `datatime`) VALUES ($creativeId,$bannerId,$country,$country,$os,$ip,$user_agent,$device_type,$device_make,$device_model,$count,'$mediatype','$start')";
			// 		OA_Dal_Delivery_query($sql1) or die(mysql_error());
				
			// }


	}
	die();

	// OA_Dal_Delivery_query($sql_res) or die(mysql_error());
	   // $resdel = array('date_time'=>array('$gte'=>$start,'$lte'=>$end));
	//to delete the selected row form MOngodb database DAC140
		// $resdelte = new MongoDB\Driver\BulkWrite;
		//$resdelte->delete($resdel);
		//$manager->executeBulkWrite(MONGODB.'.ad_response', $resdelte);
}



//response insertion end
//winning insertion start
$wincommand = new MongoDB\Driver\Command([
    'aggregate' => 'ad_response_action',
    'pipeline' => [
		[
			'$match'=>[
				'$and'=>[
					["date_time"=>[
						'$gte'=>'2019-10-01 04:00:00'
						]
					],
					["date_time"=>[
						'$lte'=>'2019-10-01 04:59:00'
						]
					]
				]
			]
		],
        [
        '$group' => [
					"_id" => [
				"country_name" => '$country_name',
				"creativeId" => '$creativeId',
				"bannerId" => '$bannerId',
				"ip" => '$ip',
				"browser_name" => '$browser.name',
				"os_name" => '$os.name',
				"device_name" => '$device.vendor',
				"device_model" => '$device.model',
				"device_type" => '$device.type',
				"country_code" => '$country_code',
				"city" => '$city',
				"region_name" => '$region_name',
				"ua" => '$ua',
				"mediaType" => '$mediaType',	
				],
				"count" => ['$sum' => 1],
				"cpm" => ['$sum' =>'$cpm'],
				
		
		]
       ],
    ],
    'cursor' => new stdClass,
]);
 
$winbids = $manager->executeCommand(MONGODB, $wincommand);
$winningbids = array();
foreach ($winbids as $windocument) {
   $winningbids[] = (array)$windocument;
}
if(is_array($winningbids) && !empty($winningbids)){	
    $winobj = array();
	for($k=0;$k<count($winningbids);$k++)
	{	$country = get_spd_value($winningbids[$k]['_id']->country_code,'country');
		//print_r($country);
		$os = get_spd_value($winningbids[$k]['_id']->os_name,'os');
		//$data['domain'] =  get_spd_value($row['domain'],'domain');
		$ip =  get_spd_value($winningbids[$k]['_id']->ip,'ip');
		$user_agent =  get_spd_value($winningbids[$k]['_id']->ua,'user_agent');
		$device_type = get_spd_value($winningbids[$k]['_id']->device_type,'device_type');
		$device_make = get_spd_value($winningbids[$k]['_id']->device_name,'device_make');
		$device_model = get_spd_value($winningbids[$k]['_id']->device_model,'device_model');
		$count=$winningbids[$k]['count'];
		$cpm=$winningbids[$k]['cpm'];
		$datatime= date('Y-m-d H:00:00');
		$mediatype = $winningbids[$k]['_id']->mediaType;

			$query12=OA_Dal_Delivery_query("SELECT id,wing_notice_count,total_amount FROM rv_dj_ba_statss WHERE  country='".$country."' AND country_name='".$country."' AND ip='".$ip."' AND user_agent='".$user_agent."' AND device_type='".$device_type."' 
		AND device_model='".$device_model."'  AND mediatype='".$mediatype."' AND datatime BETWEEN '".$sdate."' AND '".$edate."'");
		$DATA1=OA_Dal_Delivery_fetchAssoc($query12);
		if($DATA1['id'])
			{
			 $tot_count1=$DATA1['wing_notice_count']+$count;
			 $tot_cpm=$DATA1['total_amount']+$cpm;
			 OA_Dal_Delivery_query("UPDATE `rv_dj_ba_statss` SET wing_notice_count='".$tot_count1."' ,total_amount ='".$tot_cpm."' WHERE id='".$DATA1['id']."'");
			
			}

		 $sql_win="UPDATE `rv_dj_ba_statss` SET wing_notice_count='".$count."' ,total_amount ='".$cpm."' WHERE country='".$country."' AND country_name='".$country."' AND ip='".$ip."' AND user_agent='".$user_agent."' AND device_type='".$device_type."' 
		AND device_model='".$device_model."' AND mediatype='".$mediatype."'  ";
	}
	

	  // to delete the selected row form MOngodb database DAC140
	//  $windel = array('date_time'=>array('$gte'=>$start,'$lte'=>$end));
	//  $windelte = new MongoDB\Driver\BulkWrite;
	// $windelte->delete($windel);
	// $manager->executeBulkWrite(MONGODB.'.ad_response_action', $windelte);
}

function get_spd_value($value,$namer)
	{
		//print_r($value);
		$prefix = $GLOBALS['conf']['table']['prefix'];
			if($namer =='country')
			{
				$table = $prefix.'dj_country';
				$query = "SELECT id  FROM ".$table."  WHERE country_code = '".$value."' OR iso_countycode_alpha3  = '".$value."'  LIMIT 1";
				$query_data = OA_Dal_Delivery_query($query);
				$row = OA_Dal_Delivery_fetchAssoc($query_data);
				if($row)
				{
					return $row['id'];
				}
				else
				{
					return 0; 
				}				
			}
			else
			{
				 $table = $prefix.'dj_ba_'.$namer;	
				 $query=	"SELECT id FROM ".$table." WHERE ".$namer." = '".$value."' LIMIT 1";	
				 $query_data = OA_Dal_Delivery_query($query);
				 $row = OA_Dal_Delivery_fetchAssoc($query_data);
				if($row)
				{
					return $row['id'];
				}
				else
				{
					$data = array($namer=>$value);
					$columns = implode(", ", array_keys($data));
					$values =implode(", ", array_map('real_escape_string', array_values($data)) );
					OA_Dal_Delivery_query( 'INSERT INTO '.$table.' ('.$columns.') VALUES ("'.$value.'")');
					return get_spd_value($value,$namer);
				}
			}
			}


//winning insertion end
?>
