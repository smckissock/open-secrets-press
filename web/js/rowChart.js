import { biasColors } from './shared.js'; 


export class RowChart {

    constructor(facts, attribute, width, maxItems, updateFunction, title, dim) {
        this.title = title; 
        this.dim = dim ? dim : facts.dimension(dc.pluck(attribute));
        this.group = this.dim.group().reduceSum(dc.pluck('count'));

        let bars = this.dim.group().all().length;
        if (maxItems < bars) {
            bars = maxItems;
        }

        // Add div and label for the chart
        d3.select('#chart-container')
            .append('div')
            .attr('id', 'chart-' + attribute)
            .html(`<div>${title}</div>`);

        function generatePublicationColorMap(facts) {
            let publicationColorMap = {};
            facts.all().forEach(record => {
                if (!publicationColorMap[record.mediaOutlet]) {
                    publicationColorMap[record.mediaOutlet] = biasColors[record.biasRating] || '#c6dbef';
                }
            });
            return publicationColorMap;
        }
        const publicationColorMap = generatePublicationColorMap(facts);
        
        this.chart = dc.rowChart('#chart-' + attribute)
            .dimension(this.dim)
            .group(this.group)
            .data(function (d) { return d.top(maxItems); })
            .width(width)
            .height(bars * 26)
            .margins({ top: 0, right: 10, bottom: 20, left: 10 })
            .elasticX(true)
            .colors(d => {
                let color = '#c6dbef'; // Default to light blue    
                if (dc.isGillmor)
                    return color;
                
                if (attribute === 'mediaOutlet') 
                    color = publicationColorMap[d];
                if (attribute === 'biasRating')
                    color = biasColors[d];
                return color;
            })
            .label(d => `${d.key}  (${d.value.toLocaleString()})`)
            .labelOffsetX(5)
            .on('filtered', () => {
                updateFunction()
            })

        this.chart.xAxis().ticks(4).tickFormat(d3.format('.2s'));
    }
}