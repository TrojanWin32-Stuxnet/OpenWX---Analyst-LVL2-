const Panels = {
    sites: [],
    sitesByState: {},

    async init() {
        await this.loadSites();
        this.renderProductTree();
    },

    async loadSites() {
        try {
            const resp = await fetch("/api/radar/sites");
            this.sites = await resp.json();
        } catch (err) {
            console.error("Failed to load sites:", err);
            this.sites = [];
        }

        this.sitesByState = {};
        for (const site of this.sites) {
            if (!this.sitesByState[site.state]) {
                this.sitesByState[site.state] = [];
            }
            this.sitesByState[site.state].push(site);
        }
    },

    renderProductTree() {
        const container = document.getElementById("product-tree");
        container.innerHTML = "";

        const productsSection = this.createSection("Radar Products", [
            { id: "reflectivity", label: "Base Reflectivity" },
            { id: "velocity", label: "Base Velocity" },
        ], (item) => {
            App.state.activeProduct = item.id;
            this.highlightProduct(item.id);
            if (App.state.activeSite) {
                App.displayScan(App.state.currentScanIndex);
            }
        });
        container.appendChild(productsSection);

        const states = Object.keys(this.sitesByState).sort();
        for (const state of states) {
            const sites = this.sitesByState[state];
            const items = sites.map((s) => ({
                id: s.id,
                label: `${s.id} — ${s.name}`,
                data: s,
            }));

            const section = this.createSection(`${state} Sites`, items, (item) => {
                this.highlightSite(item.id);
                App.loadSite(item.id);
                RadarMap.flyToSite(item.data.lat, item.data.lon);
            });
            container.appendChild(section);
        }

        this.highlightProduct("reflectivity");
    },

    createSection(title, items, onClick) {
        const section = document.createElement("div");
        section.className = "tree-section";

        const header = document.createElement("div");
        header.className = "tree-section-header";
        header.innerHTML = `<span class="arrow">&#9654;</span> ${title}`;
        header.addEventListener("click", () => {
            header.classList.toggle("expanded");
            itemsContainer.classList.toggle("hidden");
        });
        section.appendChild(header);

        const itemsContainer = document.createElement("div");
        itemsContainer.className = "hidden";

        for (const item of items) {
            const el = document.createElement("div");
            el.className = "tree-item";
            el.dataset.id = item.id;
            el.innerHTML = `<span class="dot"></span> ${item.label}`;
            el.addEventListener("click", () => onClick(item));
            itemsContainer.appendChild(el);
        }

        section.appendChild(itemsContainer);
        return section;
    },

    highlightProduct(productId) {
        document.querySelectorAll(".tree-section:first-child .tree-item").forEach((el) => {
            el.classList.toggle("active", el.dataset.id === productId);
        });
    },

    highlightSite(siteId) {
        document.querySelectorAll(".tree-item").forEach((el) => {
            if (el.closest(".tree-section:first-child")) return;
            el.classList.toggle("active", el.dataset.id === siteId);
        });
    },
};
