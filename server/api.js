'use strict';

const express = require('express');
const hbase = require('hbase-rpc-client');
const Canvas = require('canvas');
const router = express.Router();

/**
 * Size of image is 250px
 */
const IMAGE_SIZE = 250;

const SEPARATOR = ',';

const MIN_COLOR = {
    r: 4,
    g: 139,
    b: 154
}

const MAX_COLOR = {
    r: 255,
    g: 255,
    b: 255
}

const MIN_ELEVATION = -160;
const MAX_ELEVATION = 7430;

const HBASE_TABLE = 'BounaderMarzinTable';

router.get('/map-blocks', (req, res) => {

    const latitude = req.query.latitude;
    const longitude = req.query.longitude;

    const originLatitude = latitude - (latitude % IMAGE_SIZE);
    const originLongitude = longitude - (longitude % IMAGE_SIZE);

    const client = hbase({		
        zookeeperHosts: ['beetlejuice'],
        tcpNoDelay: true		
    });		
    client.on('error', err => console.error('HBASE error: ' + err));

    const get = new hbase.Get(new Buffer(`${originLatitude},${originLongitude}`));
    
    client.get(HBASE_TABLE, get, (err, res) => {

        const canvas = new Canvas(IMAGE_SIZE, IMAGE_SIZE);
        const context = canvas.getContext("2d");
        const imgData = context.createImageData(IMAGE_SIZE, IMAGE_SIZE);

        for (let l = 0; l < IMAGE_SIZE; l++) {
            for (let c = 0; c < IMAGE_SIZE; c++) {
                const pixelName = `pixels:${l}${SEPARATOR}${c}`;
                const pixelExists = !!res && 
                                    !!res.cols &&
                                    !!res.cols[pixelName] &&
                                    (res.cols[pixelName].value instanceof Buffer);

                const linearPixelPos = (c * IMAGE_SIZE + l) * 4;
                const currentColor = Object.assign({}, MIN_COLOR);

                if (pixelExists) {
                    const elevationStr = res.cols[pixelName].value.toString();
                    const elevation = parseInt(elevationStr);
                    if (!isNaN(elevation)) {
                        currentColor.r = ((MAX_COLOR.r - MIN_COLOR.r) * (elevation - MIN_ELEVATION) / (MAX_ELEVATION - MIN_ELEVATION)) + MIN_COLOR.r;
                        currentColor.g = ((MAX_COLOR.g - MIN_COLOR.g) * (elevation - MIN_ELEVATION) / (MAX_ELEVATION - MIN_ELEVATION)) + MIN_COLOR.g;
                        currentColor.b = ((MAX_COLOR.b - MIN_COLOR.b) * (elevation - MIN_ELEVATION) / (MAX_ELEVATION - MIN_ELEVATION)) + MIN_COLOR.b;
                    }
                }
                
                imgData.data[linearPixelPos] = currentColor.r;
                imgData.data[linearPixelPos + 1] = currentColor.g;
                imgData.data[linearPixelPos + 2] = currentColor.b;
                imgData.data[linearPixelPos + 3] = 255;
            }
        }

        context.putImageData(imgData, 0, 0);
        
        res.setHeader('Content-Type', 'image/png');
        canvas.pngStream().pipe(res);
    });
});

module.exports = router;