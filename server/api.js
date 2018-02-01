"use strict";

const express = require('express');
const hbase = require('hbase-rpc-client');
const router = express.Router();
/*

							int r = 251 * (elevation - (-160)) / (7430 - (-160)) + 4;
							int g = 116 * (elevation - (-160)) / (7430 - (-160)) + 139;
                            int b = 101 * (elevation - (-160)) / (7430 - (-160)) + 154;
                            */


/*
    Integer posXBlock = posX - (posX % Constants.SIZE_BLOCK);
    Integer posYBlock = posY - (posY % Constants.SIZE_BLOCK);
*/
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

router.get('/map-blocks', (req, res) => {
    // TODO calculate good coordinate
    const get = new hbase.Get(new Buffer('0.3,-78.975'));
    
    client.get ('BounaderMarzinTable', get, (err, res) => {

        const canvas = new Canvas(IMAGE_SIZE, IMAGE_SIZE);
        const context = canvas.getContext("2d");
        const imgData = ctx.createImageData(IMAGE_SIZE, IMAGE_SIZE);

        /*for (var i = 0; i < imgData.data.length; i += 4){
            imgData.data[i+0]=255;
            imgData.data[i+1]=0;
            imgData.data[i+2]=0;
            imgData.data[i+3]=255;
        }*/

        for (let l = 0; l < IMAGE_SIZE; l++) {
            for (let c = 0; c < IMAGE_SIZE; l++) {
                const pixelName = `pixels:${l}${SEPARATOR}${c}`;
                pixelExists = !!res && 
                              !!res.cols &&
                              !!res.cols[pixelName] &&
                              (res.cols[pixelName].value instanceof Buffer);

                const linearPixelPos = 0; // TODO * 4 (rgba)
                const currentColor = Object.assign({}, MIN_COLOR);

                if (pixelExists) {
                    const elevationStr = res.cols[pixelName].value.toString();
                    const elevation = parseInt(elevationStr);
                    if (!isNaN(elevation)) {
                        currentColor.r = ((MAX_COLOR.r - MIN_COLOR.r) * (elevation - MIN_ELEVATION) / (MAX_ELEVATION - MIN_ELEVATION))+ MIN_COLOR.r;
                        currentColor.g = ((MAX_COLOR.g - MIN_COLOR.g) * (elevation - MIN_ELEVATION) / (MAX_ELEVATION - MIN_ELEVATION))+ MIN_COLOR.g;
                        currentColor.b = ((MAX_COLOR.b - MIN_COLOR.b) * (elevation - MIN_ELEVATION) / (MAX_ELEVATION - MIN_ELEVATION)) + MIN_COLOR.b;
                    }
                }
                
                imgData.data[linearPixelPos] = currentColor.r;
                imgData.data[linearPixelPos + 1] = currentColor.g;
                imgData.data[linearPixelPos + 2] = currentColor.b;
                imgData.data[linearPixelPos + 3] = 255;
            }
        }
        // TODO send image
        ctx.putImageData(imgData,10,10);
        // TODO set headers
    });
});

module.exports = router;