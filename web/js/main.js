import {formatDate, addCommas, scrollToTop, biasColors} from './shared.js';
import {RowChart} from './rowChart.js'; 
import {loadParquetData} from './dataLoader.js';


export class Site {

  constructor() {
    if (window.location.hostname === '127.0.0.1') 
            document.title = 'OpenSecrets DEV';

        const overlay = document.getElementById('loading-overlay');
        overlay.classList.replace('loading-hidden','loading-visible');
        this.getData().then(stories => {
            this.stories = stories;
            this.stories.forEach(story => {
                story.count = 1;
                story.date = new Date(story.publishDate);
                if (story.title == '') {
                    story.title = 'Link to story';
                }               
            });
            this.facts = new crossfilter(stories);
            dc.facts = this.facts;

            this.setupCharts();
            dc.renderAll();
            
            overlay.classList.replace('loading-visible','loading-hidden'); 
            this.refresh();
        });
        window.site = this;        
    }

    async getData() {
        return await loadParquetData('data/stories.parquet');
    }

    refresh() {
        window.site.listStories();

        const stories = dc.facts.allFiltered().length;
        let filters = [];   
        dc.chartRegistry.list().forEach(chart => {
            chart.filters().forEach(filter => filters.push(filter));
        });

        d3.select('#filters')
            .html(`
                <button id='clear-filters' class='clear-button'>Clear All</button>
                <span class='case-count'> ${addCommas(stories)} OpenSecrets citations</span> &nbsp;
                <span class='case-filters'>${filters.join(', ')}</span>
            `);

        d3.select('#clear-filters').on('click', function() {
            dc.filterAll();
            dc.redrawAll()
            dc.refresh();
            window.site.listStories();
        });
    }

    setupCharts() {
        dc.refresh = this.refresh;
        new RowChart(this.facts, 'mediaOutlet', dc.leftWidth, 160, this.refresh, 'Media Outlet', null);
        new RowChart(this.facts, 'biasRating', 160, 6, this.refresh, 'Political Orientation', null);
        new RowChart(this.facts, 'mediaOutletType', 200, 9, this.refresh, 'Media Type', null);
        new RowChart(this.facts, 'state', 200, 100, this.refresh, 'State/Country', null);
    }

    listStories() {
        const storiesToShow = 60;
        function storyResult(d) {
            console.log(d.biasRating + '  ' + biasColors[d.biasRating])
            return `
              <div class="story" onclick="window.open('${d.url}', '_blank', 'noopener')">
                <img
                  class="story-image"
                  src="${d.image}"
                  onload="this.classList.add('loaded')"
                  onerror="this.style.display='none'"
                  height="90"
                  width="120"
                >
                <div class="story-body">
                  <h5 class="story-topic">
                    <span class="media-outlet">${d.mediaOutlet}</span><span class="date-authors"> &nbsp; ${formatDate(d.date)}   ${d.authors}</span>
                    <span style="float:right;color:${biasColors[d.biasRating]||'#333'}">
                      ${d.biasRating}
                    </span>
                  </h5>
                  <h3 class="story-title">${d.title}</h3>
                  <p class="story-excerpt">

                    ${d.sentence.replace(
                      /OpenSecrets/gi,
                      `<b><span style="color:#000">OpenSecrets</span></b>`
                    )}

                  </p>
                </div>
              </div>
            `;
          }

        let html = this.facts.allFiltered()
            .sort((a, b) =>
                new Date(b.date) - new Date(a.date)
            )
            .slice(0, storiesToShow)
            .map(d => storyResult(d))
            .join('');

        d3.select('#chart-list')
            .html(html);
    }       
}

new Site(); 