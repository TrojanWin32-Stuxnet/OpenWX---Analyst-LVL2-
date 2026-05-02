const RadarMap = {
    map: null,
    overlay: null,
    radarLayer: null,

    async init(containerId) {
        this.map = new maplibregl.Map({
            container: containerId,
            style: "https://basemaps.cartocdn.com/gl/dark-matter-gl-style/style.json",
            center: [-97.5, 38.0],
            zoom: 4.5,
            attributionControl: false,
        });

        this.map.addControl(
            new maplibregl.AttributionControl({ compact: true }),
            "bottom-right"
        );

        this.map.addControl(
            new maplibregl.NavigationControl({ showCompass: false }),
            "top-right"
        );

        this.overlay = new deck.MapboxOverlay({
            layers: [],
        });
        this.map.addControl(this.overlay);

        await new Promise((resolve) => {
            this.map.on("load", resolve);
        });

        console.log("Map initialized.");
    },

    setRadarOverlay(imageUrl, bounds, siteId) {
        this.radarLayer = new deck.BitmapLayer({
            id: `radar-${siteId}`,
            bounds: [bounds.west, bounds.south, bounds.east, bounds.north],
            image: imageUrl,
            opacity: 0.75,
            pickable: false,
        });
        this.overlay.setProps({
            layers: [this.radarLayer],
        });
    },

    clearRadarOverlay() {
        this.overlay.setProps({ layers: [] });
        this.radarLayer = null;
    },

    flyToSite(lat, lon) {
        this.map.flyTo({
            center: [lon, lat],
            zoom: 7,
            speed: 1.5,
        });
    },
};
