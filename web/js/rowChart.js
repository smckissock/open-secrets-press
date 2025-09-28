import { biasColors } from './shared.js'; 


export class RowChart {

    constructor(facts, attribute, width, maxItems, updateFunction, title, dim, noUnspecified) {
        this.dim = dim ? dim : facts.dimension(dc.pluck(attribute));
        this.group = this.dim.group().reduceSum(dc.pluck('count'));
        if (noUnspecified)
            this.group = removeUnspecified(this.group);

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
                if (!publicationColorMap[record.publication]) {
                    publicationColorMap[record.publication] = biasColors[record.bias] || '#c6dbef';
                }
            });
            return publicationColorMap;
        }
        const publicationColorMap = generatePublicationColorMap(facts);

        dc.rowChart('#chart-' + attribute)
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
                
                if (attribute === 'publication') 
                    color = publicationColorMap[d];
                if (attribute === 'bias')
                    color = biasColors[d];
                return color;
            })
            .label(d => `${d.key}  (${d.value.toLocaleString()})`)
            .labelOffsetX(5)
            .on('filtered', () => {
                updateFunction()
            })
            .xAxis().ticks(4).tickFormat(d3.format('.2s'));

        // Note: if ever want to exclude something other than 'Unspecified' we could change this to pass it in.
        function removeUnspecified(group) {
            let predicate = d => d.key !== 'Unspecified';

            return {
                all: function () {
                    return group.all().filter(d => predicate(d));
                },
                top: function (n) {
                    return group.top(Infinity)
                        .filter(predicate)
                        .slice(0, n);
                }
            };
        }
    }

    // Note: if ever want to exclude something other than 'Unspecified' we could change this to pass it in.
    removeUnspecified(group) {
        let predicate = d => d.key !== 'Unspecified';
        return {
            all: function () {
                return group.all().filter(d => predicate(d));
            },
            top: function (n) {
                return group.top(Infinity)
                    .filter(predicate)
                    .slice(0, n);
            }
        };
    }
}