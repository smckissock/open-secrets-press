import {formatDate, addCommas, scrollToTop, biasColors} from './shared.js';


export class Site {

    constructor() {
        if (window.location.hostname === '127.0.0.1') 
            document.title = 'OpenSecrets DEV';

        this.configure();                
        this.getData().then(stories => {
            this.stories = stories;
            this.facts = new crossfilter(stories);
            decodeURI.facts = this.facts;
            this.refresh();
        });
        window.site = this;        
    }

    configure() {
    }

    async getData() {
        const res = await fetch("./data/stories.csv", { cache: 'no-store' });
        const stories = await res.text();
        return d3.csvParse(stories);   
    }

    refresh() {
        this.listStories()
    }

    listStories() {
        const storiesToShow = 60;
        function storyResult(d) {
            return `
              <div class="story" onclick="window.open('${d.url}', '_blank', 'noopener')">          
                <div class="story-body">
                  <h5 class="story-topic">
                    <strong>${d.media_name}</strong> 
                  </h5>
                  <h3 class="story-title">${d.title}</h3>
                </div>
              </div>
            `;
          }

        let html = this.facts.allFiltered()
            // .sort((a, b) =>
            //     new Date(b.date) - new Date(a.date)
            // )
            .slice(0, storiesToShow)
            .map(d => storyResult(d))
            .join('');

        d3.select('#chart-list')
            .html(html);
    }   
}

new Site(); 