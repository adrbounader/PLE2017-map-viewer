const express = require('express');
const hbase = require('hbase-rpc-client');
const router = express.Router();

router.get('/map-blocks', (req, res) => {
    console.log ("------------ APP DEBUT")
    /*var Canvas = require('canvas');
    
    var canvas = new Canvas(250, 250);
    var context = canvas.getContext("2d");
    var imgData=ctx.createImageData(100,100);
    for (var i=0;i<imgData.data.length;i+=4)
    {
    imgData.data[i+0]=255;
    imgData.data[i+1]=0;
    imgData.data[i+2]=0;
    imgData.data[i+3]=255;
    }
  ctx.putImageData(imgData,10,10);*/
    const client = hbase({
        zookeeperHosts: ['beetlejuice'],
        tcpNoDelay: true
    });
    client.on('error', err => console.error('HBASE error: ' + err));

    const scan = client.getScanner('BounaderMarzinTable');
    /*const filtersList = new hbase.FilterList();
    filtersList.addFilter({
        singleColumnValueFilter: {
            columnFamily: "coor"
        }
    })*//*
    console.log (scan.setFilter({
        singleColumnValueFilter: {
            columnFamily: "coor"
        }
    }));
    */
    //console.log (hbase.FilterList.Operator)

    get = new hbase.Get("cle");
    
    client.get ("BounaderMarzinTable", get, (err, res) => {
        console.log (res)
        console.log (err)
    });
/*
    scan.next((err, row) => {
        console.log (row);
    })
    /*
    
    client.get('BounaderMarzinTable', get, (err, res) => {
        console.log (res);
        console.log (err);
    });*/
    res.send("OK");
    console.log ("------------ APP FIN")
});

module.exports = router;