import {formatDate, addCommas, scrollToTop, biasColors} from './shared.js';
import {RowChart} from './rowChart.js';

// Not used. Expensive to download
// import {loadParquetData} from './dataLoader.js';

async function loadGzipCsv(url) {
  const { gunzipSync } = await import('https://cdn.jsdelivr.net/npm/fflate@0.8.2/esm/browser.js');

  const res = await fetch(url);
  if (!res.ok) throw new Error(`Fetch failed for ${url}: ${res.status} ${res.statusText}`);

  const gz = new Uint8Array(await res.arrayBuffer());
  const csvBytes = gunzipSync(gz);
  const csvText = new TextDecoder().decode(csvBytes);

  return d3.csvParse(csvText);
}

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
    // Brotli would be a better choice, but can';'t control headers on github pages. 
    return await loadGzipCsv('data/stories.csv.gz');

    // Parquet is wrong choice for smalller mostly text csvs. Extra time to load parquetjs-wasm and decode
    // const { loadParquetData } = await import('./dataLoader.js');
    // return await loadParquetData('data/stories.parquet');
  }

  setupCharts() {
    dc.refresh = this.refresh;

    this.rowCharts = [
      new RowChart(this.facts, 'mediaOutlet', dc.leftWidth, 160, this.refresh, 'Media Outlet', null),
      new RowChart(this.facts, 'biasRating', 160, 6, this.refresh, 'Political Orientation', null),
      new RowChart(this.facts, 'mediaOutletType', 200, 9, this.refresh, 'Media Type', null),
      new RowChart(this.facts, 'country', 200, 100, this.refresh, 'Country', null),
      new RowChart(this.facts, 'state', 200, 100, this.refresh, 'State', null)
    ];
  }

  refresh() {
    window.site.listStories();
    window.site.showFilters();

    d3.select('#clear-filters').on('click', function() {
      dc.filterAll();
      dc.redrawAll();
      dc.refresh();
      window.site.listStories();
    });
  }

  downloadCsv() {
    const filteredData = this.facts.allFiltered();
    
    // Define CSV headers
    const headers = ['mediaOutlet', 'biasRating', 'mediaOutletType', 'state', 'publishDate', 'authors', 'title', 'sentence', 'url', 'image'];
    
    // Create CSV content
    const csvRows = [headers.join(',')];
    
    filteredData.forEach(story => {
      const row = headers.map(header => {
        let value = story[header] || '';
        // Escape quotes and wrap in quotes if contains comma, quote, or newline
        if (typeof value === 'string') {
          value = value.replace(/"/g, '""');
          if (value.includes(',') || value.includes('"') || value.includes('\n')) {
            value = `"${value}"`;
          }
        }
        return value;
      });
      csvRows.push(row.join(','));
    });
    
    const csvContent = csvRows.join('\n');
    const blob = new Blob([csvContent], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.setAttribute('href', url);
    link.setAttribute('download', `opensecrets_stories_${new Date().toISOString().split('T')[0]}.csv`);
    link.style.visibility = 'hidden';
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
  }

  showFilters() {
    const filterTypes = [];
    this.rowCharts.forEach(rowChart => {
      const chartFilters = rowChart.chart.filters();
      if (chartFilters.length > 0) {
        filterTypes.push({
          name: rowChart.title,
          filters: chartFilters
        });
      }
    });

    const filterBoxes = filterTypes.map(filterType => `
      <div class='filter-box'>
        <div class='filter-box-title'>${filterType.name}</div>
        <div class='filter-box-values'>${filterType.filters.join(', ')}</div>
      </div>
    `).join('');

    let clearButton = "";
    if (filterTypes.length > 0) {
      const filtersString = filterTypes.length == 1 ? "filter" : "filters";
      clearButton = `<button id='clear-filters' class='clear-button'>Clear ${filtersString}</button>`;
    }
    
    const storyCount = dc.facts.allFiltered().length;
    d3.select('#filters').html(`
        <div style='display: flex; justify-content: space-between; align-items: flex-start; width: 100%;'>
            <div style='display: flex; flex-direction: column; gap: 10px;'>
                <span class='case-count'>${addCommas(storyCount)} OpenSecrets citations</span>
                <div class='filter-boxes-container'>${filterBoxes}${clearButton}</div>
            </div>
            <div class='right-links'>
                <a href='#' id='download-csv' class='github-link'>Download</a>
                <a href='https://github.com/smckissock/open-secrets-press' target='_blank' rel='noopener noreferrer' class='github-link'>GitHub</a>
            </div>
        </div>
    `);

    // Add event listener for download link
    d3.select('#download-csv').on('click', (event) => {
      event.preventDefault();
      this.downloadCsv();
    });
  }

  listStories() {
    const storiesToShow = 60;
    function storyResult(d) {
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
              ${d.sentence.replace(/OpenSecrets/gi, `<b><span style="color:#000">OpenSecrets</span></b>`)}
            </p>
          </div>
        </div>
      `;
    }

    let html = this.facts.allFiltered()
      .sort((a, b) => new Date(b.date) - new Date(a.date))
      .slice(0, storiesToShow)
      .map(d => storyResult(d))
      .join('');

    d3.select('#chart-list').html(html);
  }
}

new Site();