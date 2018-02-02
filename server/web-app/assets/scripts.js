const NB_BLOCKS_ROW = 8;
const NB_BLOCKS_COLUMN = 10;
const LAT_LNG_DELTA = 0.250;

let INIT_LAT = 43.29819;
let INIT_LNG = -1.20849;

const moveToFun = {
    moveToLeft() {
        const blocksToLoad = [];
        for(let r = 0; r < NB_BLOCKS_ROW; r++) {
            const $mapRow = $('.map-row').eq(r);
            const $lastBlock = $mapRow.children().last();

            $lastBlock.attr('src', '');
            $mapRow.prepend($lastBlock.detach());

            blocksToLoad.push({ r, c: 0 });
        }

        INIT_LNG -= LAT_LNG_DELTA;
        loadMapBlocks(blocksToLoad);
    },
    moveToRight() {
        const blocksToLoad = [];
        for(let r = 0; r < NB_BLOCKS_ROW; r++) {
            const $mapRow = $('.map-row').eq(r);
            const $firstBlock = $mapRow.children().first();

            $firstBlock.attr('src', '');
            $mapRow.append($firstBlock.detach());

            blocksToLoad.push({ r, c: (NB_BLOCKS_COLUMN - 1) });
        }

        INIT_LNG += LAT_LNG_DELTA;
        loadMapBlocks(blocksToLoad);
    },
    moveToUp() {
        const blocksToLoad = [];
        const $lastMapRow = $('.map-row').last();

        for(let c = 0; c < NB_BLOCKS_COLUMN; c++) {
            const $mapBlock = $lastMapRow.children().eq(c);
            $mapBlock.attr('src', '');

            blocksToLoad.push({ r: 0, c });
        }

        $('.map-container').prepend($lastMapRow);

        INIT_LAT -= LAT_LNG_DELTA;
        loadMapBlocks(blocksToLoad);
    },
    moveToDown() {
        const blocksToLoad = [];
        const $firstMapRow = $('.map-row').first();

        for(let c = 0; c < NB_BLOCKS_COLUMN; c++) {
            const $mapBlock = $firstMapRow.children().eq(c);
            $mapBlock.attr('src', '');

            blocksToLoad.push({ r: (NB_BLOCKS_ROW - 1), c });
        }

        $('.map-container').append($firstMapRow);

        INIT_LAT += LAT_LNG_DELTA;
        loadMapBlocks(blocksToLoad);
    }
};

/**
 * Return the wanted jquery block.
 * @param {number} r row of the wanted block
 * @param {number} c column of the wanted block
 */
function getMapBlock(r, c) {
    if(r < 0 || r >= NB_BLOCKS_ROW || c < 0 || c >= NB_BLOCKS_COLUMN) {
        throw new Error(`Invalid coordinates: ${JSON.stringify(coordinates)}`)
    }
    return $('.map-row').eq(r).children().eq(c);
}

function loadAllMapBlocks() {
    const blocksToLoad = [];
    for(let r = 0; r < NB_BLOCKS_ROW; r++) {
        for(let c = 0; c < NB_BLOCKS_COLUMN; c++) {
            blocksToLoad.push({ r, c });
        }
    }
    loadMapBlocks(blocksToLoad);
}

/**
 * Load block which coordinates have been given in parameter.
 * @param {Array<{r: number, c: number}>} blocksToLoad Array of object containing coordinates of block to load
 */
function loadMapBlocks(blocksToLoad) {
    for(const coordinates of blocksToLoad) {
        if(coordinates.r < 0 || coordinates.r >= NB_BLOCKS_ROW ||
           coordinates.c < 0 || coordinates.c >= NB_BLOCKS_COLUMN) {
            throw new Error(`Invalid coordinates: ${JSON.stringify(coordinates)}`)
        }
        
        const latLongCoordinates = { 
            r: INIT_LAT + coordinates.r * LAT_LNG_DELTA,
            c: INIT_LNG + coordinates.c * LAT_LNG_DELTA
        };

        const $mapBlock = getMapBlock(coordinates.r, coordinates.c);
        $mapBlock.attr('src', `/api/map-blocks?latitude=${latLongCoordinates.r}&longitude=${latLongCoordinates.c}`);
    }
}

function generateHTMLMapBlocks() {

    const $container = $('.map-container');
    const screenHeight = $(window).height();
    const screenWidth = $(window).width();

    $container.empty();

    for (let x = 0; x < NB_BLOCKS_ROW; x++) {
        const $row = $('<div/>', { class: 'map-row' });
        
        for (let y = 0; y < NB_BLOCKS_COLUMN; y++) {
            const $block = $('<img/>', {
                class: 'map-block',
                alt: 'Loading...',
                'data-num': (y * NB_BLOCKS_COLUMN + x)
            })

            $block.height(screenHeight / NB_BLOCKS_ROW);
            $block.width(screenWidth / NB_BLOCKS_COLUMN);

            $row.append($block);
        }

        $container.append($row);
    }
}

$(function () {
    generateHTMLMapBlocks();
    loadAllMapBlocks();

    $('.toolkit > .btn').click(function() {
        const name = $(this).attr('name');
        console.log (name);
        moveToFun[`moveTo${name.charAt(0).toUpperCase()}${name.substr(1)}`]();
    });
});