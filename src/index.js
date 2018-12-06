// const XLSX = require( 'xlsx' )
// var getData = require( 'getData' )
var transBd2Gps = require( 'dealData' ).transBd2Gps

var fs = require( 'fs' )
var _ = require( 'lodash' )

const PATH = 'D:////WORKSPACE1/ChinaRetailersGPS/ChinaRetailersGPS.csv'
const RLT_PATH = 'D:////WORKSPACE1/ChinaRetailersGPS/ChinaRetailersGPS_rlt.csv'

const input = fs.createReadStream( `${PATH}` );
const output = fs.createWriteStream( `${RLT_PATH}` );

// pcom_id,ccom_id,nation_cust_code,cust_name,busi_addr,license_code,longitude,latitude,bdlongitude,bdlatitude,bdprecise,bdconfidence,bdcomprehension,bdLevelType,is_checked
const dealRow = ( str )=>{
	let cols = _.split( str, ',' )
	let longitude = cols[6]
	let latitude = cols[7]
	let bdlongitude = cols[8]
	let bdlatitude = cols[9]
	if( !longitude || !latitude ){
		// console.log( '此数据没有经纬度 ' + str )
		let rlt = transBd2Gps( bdlongitude, bdlatitude )
		cols[6] = rlt[0]
		cols[7] = rlt[1]
		// console.log( cols.join( ',' ) )
	}
	let rltStr = cols.join( ',' )
	return rltStr
}

var custCount = 0
var errCustCount = 0
var lastStr = '' // 每次读取不一定完全读取整行，可能会落下一部分，把多读取的缓存起来

input.on( 'data',function(data){
	// console.log( 'begin---------------------------' )

	let tmpD = data.toString( 'utf16le' )
	let tmpArr = _.split(tmpD, '\n')

	custCount += tmpArr.length

	let dealRlt = []
	_.forEach( tmpArr, ( d, idx )=>{
		if( idx == 0 ){
			let rltStr = dealRow( lastStr + d )
			lastStr = '' // 用完之后要赋空值
			if( rltStr.indexOf( ',,,,' ) != -1 ){
				errCustCount++
				console.log( 'errCustCount = ' + errCustCount )
			}else{
				dealRlt.push( rltStr ) // 第一行不一定是全的，可能又上次读剩下的
			}
		}else{
			let matchs = _.endsWith( d, 'True\r' ) || _.endsWith( d, 'False\r' )
			if( matchs ){
				let rltStr = dealRow( d )
				if( rltStr.indexOf( ',,,,' ) != -1 ){
					errCustCount++
					console.log( 'errCustCount = ' + errCustCount )
				}else{
					dealRlt.push( rltStr ) // 第一行不一定是全的，可能又上次读剩下的
				}
			}else{
				// console.log( "最后一行数据" + d )
				lastStr = d // 最后一行不全的情形下，不往结果里写
			}
		}
	})

	// console.log( `已处理零售户数据---------------------------${custCount}` )

	output.write( Buffer.from( dealRlt.join( '\n' ), 'utf16le' ) );

	// if( custCount >= 120000 ) // 测试用
	// 	output.end();
} );

input.on( 'end',function(){
	console.log('readStream end');
	output.end();
} )

// input.pipe(decipher).pipe(output);

// let datas = getData( PATH )
// dealData( datas )
