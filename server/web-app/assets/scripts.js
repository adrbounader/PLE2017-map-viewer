const NB_BLOCKS_ROW = 8;
const NB_BLOCKS_COLUMN = 10;

$(function () {
    generateMapBlocks();
});

function generateMapBlocks() {

    const $container = $('.map-container');
    const screenHeight = $(window).height();
    const screenWidth = $(window).width();

    $container.empty();

    for (let x = 0; x < NB_BLOCKS_ROW; x++) {
        const $row = $('<div/>', { class: 'map-row' });
        
        for (let y = 0; y < NB_BLOCKS_COLUMN; y++) {
            const $block = $('<div/>', { class: 'map-block' })

            $block.height(screenHeight / NB_BLOCKS_ROW);
            $block.width(screenWidth / NB_BLOCKS_COLUMN);

            $row.append($block);
        }

        $container.append($row);
    }
}