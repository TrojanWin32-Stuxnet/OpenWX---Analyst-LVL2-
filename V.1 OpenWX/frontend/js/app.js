const App = {
    state: {
        activeSite: null,
        activeProduct: "reflectivity",
        activeSweep: 0,
        scans: [],
        currentScanIndex: 0,
        playing: false,
        refreshTimer: null,
        fetchTimer: null,
        lastDisplayFailed: false,
        settings: {
            pollIntervalSeconds: 120,
            initialScanCount: 30,
            maxScans: 120,
            maxAllowedScans: 120,
        },
    },

    log(msg, data) {
        const ts = new Date().toLocaleTimeString();
        if (data !== undefined) {
            console.log(`[OpenGR ${ts}] ${msg}`, data);
        } else {
            console.log(`[OpenGR ${ts}] ${msg}`);
        }

        const el = document.getElementById("inspector-content");
        if (el) {
            const line = document.createElement("div");
            line.style.cssText = "font-size:10px;color:#8888aa;border-bottom:1px solid #2a2a44;padding:2px 0;";
            line.textContent = `${ts} ${msg}`;
            el.appendChild(line);
            el.scrollTop = el.scrollHeight;
        }
    },

    clampScanCount(value, fallback) {
        const parsed = Number.parseInt(value, 10);
        if (Number.isNaN(parsed)) {
            return fallback;
        }
        const maxAllowed = this.state.settings.maxAllowedScans || 120;
        return Math.min(maxAllowed, Math.max(1, parsed));
    },

    loadSavedFeedSettings() {
        try {
            return JSON.parse(window.localStorage.getItem("opengr.radarFeedSettings") || "{}");
        } catch {
            return {};
        }
    },

    persistFeedSettings() {
        const payload = {
            initialScanCount: this.state.settings.initialScanCount,
            maxScans: this.state.settings.maxScans,
        };
        window.localStorage.setItem("opengr.radarFeedSettings", JSON.stringify(payload));
    },

    renderFeedControls() {
        const initialInput = document.getElementById("initial-scan-count");
        const maxInput = document.getElementById("max-scan-count");
        if (!initialInput || !maxInput) {
            return;
        }
        initialInput.max = String(this.state.settings.maxAllowedScans);
        maxInput.max = String(this.state.settings.maxAllowedScans);
        initialInput.value = String(this.state.settings.initialScanCount);
        maxInput.value = String(this.state.settings.maxScans);
    },

    bindFeedControls() {
        const applyBtn = document.getElementById("apply-feed-settings");
        if (!applyBtn) {
            return;
        }

        applyBtn.addEventListener("click", async () => {
            const initialInput = document.getElementById("initial-scan-count");
            const maxInput = document.getElementById("max-scan-count");

            const initialScanCount = this.clampScanCount(
                initialInput?.value,
                this.state.settings.initialScanCount,
            );
            const maxScans = this.clampScanCount(
                maxInput?.value,
                this.state.settings.maxScans,
            );

            this.state.settings.initialScanCount = initialScanCount;
            this.state.settings.maxScans = Math.max(initialScanCount, maxScans);
            this.persistFeedSettings();
            this.renderFeedControls();

            this.log(
                `Feed settings applied: initial ${this.state.settings.initialScanCount}, max ${this.state.settings.maxScans}`,
            );

            if (this.state.activeSite) {
                await this.loadSite(this.state.activeSite);
            }
        });
    },

    async loadSettings() {
        try {
            const resp = await fetch("/api/radar/settings");
            if (!resp.ok) {
                throw new Error(`settings ${resp.status}`);
            }
            const settings = await resp.json();
            const saved = this.loadSavedFeedSettings();
            const initialScanCount = this.clampScanCount(
                saved.initialScanCount ?? settings.default_initial_scan_count,
                settings.default_initial_scan_count,
            );
            const maxScans = this.clampScanCount(
                saved.maxScans ?? settings.default_max_scans,
                settings.default_max_scans,
            );

            this.state.settings = {
                pollIntervalSeconds: settings.poll_interval_seconds,
                initialScanCount,
                maxScans: Math.max(initialScanCount, maxScans),
                maxAllowedScans: settings.max_allowed_scans,
            };
            this.persistFeedSettings();
            this.renderFeedControls();
            this.log(
                `Radar settings loaded (initial ${initialScanCount}, max ${this.state.settings.maxScans}, poll ${settings.poll_interval_seconds}s)`,
            );
        } catch (err) {
            this.log(`SETTINGS LOAD FAILED: ${err.message}`);
            this.renderFeedControls();
        }
    },

    async init() {
        this.log("Initializing...");
        await this.loadSettings();
        this.bindFeedControls();

        try {
            await RadarMap.init("map");
            this.log("Map ready");
        } catch (err) {
            this.log("MAP INIT FAILED: " + err.message);
            console.error(err);
        }

        try {
            await Panels.init();
            this.log("Panels ready");
        } catch (err) {
            this.log("PANELS INIT FAILED: " + err.message);
            console.error(err);
        }

        Timeline.init();
        this.log("Timeline ready");
        WS.init();
        this.log("WebSocket connecting...");
        this.setStatus("Ready");
        this.log("Init complete");
    },

    setStatus(text) {
        const statusBar = document.getElementById("status-bar");
        if (statusBar) {
            statusBar.textContent = text;
        }
    },

    stopLivePolling() {
        if (this.state.refreshTimer) {
            clearInterval(this.state.refreshTimer);
            this.state.refreshTimer = null;
        }
        if (this.state.fetchTimer) {
            clearInterval(this.state.fetchTimer);
            this.state.fetchTimer = null;
        }
    },

    startLivePolling() {
        this.stopLivePolling();
        this.state.refreshTimer = setInterval(() => this.refreshScans(), 3000);
        this.state.fetchTimer = setInterval(
            () => this.requestFetch("poll"),
            this.state.settings.pollIntervalSeconds * 1000,
        );
    },

    async requestFetch(reason = "manual") {
        const siteId = this.state.activeSite;
        if (!siteId) {
            return null;
        }

        const params = new URLSearchParams({
            count: String(this.state.settings.initialScanCount),
            max_scans: String(this.state.settings.maxScans),
        });

        const fetchResp = await fetch(`/api/radar/${siteId}/fetch?${params.toString()}`, {
            method: "POST",
        });
        if (!fetchResp.ok) {
            throw new Error(`fetch ${fetchResp.status}`);
        }

        const fetchData = await fetchResp.json();
        this.log(`fetch(${reason}) response:`, fetchData);
        return fetchData;
    },

    async loadSite(siteId) {
        this.state.activeSite = siteId;
        this.state.scans = [];
        this.state.currentScanIndex = 0;
        this.state.lastDisplayFailed = false;
        Timeline.stopPlay?.();
        this.stopLivePolling();

        this.setStatus(
            `Loading ${siteId} (${this.state.settings.initialScanCount} recent, max ${this.state.settings.maxScans})...`,
        );
        this.log(
            `loadSite(${siteId}) — backfill ${this.state.settings.initialScanCount}, rolling max ${this.state.settings.maxScans}`,
        );

        try {
            await this.requestFetch("initial");
            await this.refreshScans({ forceLatest: true });
            this.startLivePolling();
        } catch (err) {
            this.log(`loadSite ERROR: ${err.message}`);
            console.error("Failed to load site:", err);
            this.setStatus(`Error loading ${siteId}`);
        }
    },

    async refreshScans({ forceLatest = false } = {}) {
        const site = this.state.activeSite;
        if (!site) return;

        try {
            const prevScans = this.state.scans;
            const prevCount = prevScans.length;
            const prevLatestScanTime = prevScans[0]?.scan_time ?? null;
            const prevIndex = this.state.currentScanIndex;
            const prevDisplayedScanTime = prevScans[prevIndex]?.scan_time ?? null;

            const resp = await fetch(`/api/radar/${site}/scans?limit=${this.state.settings.maxScans}`);
            if (!resp.ok) {
                throw new Error(`scans ${resp.status}`);
            }
            const scans = await resp.json();
            this.state.scans = scans;
            Timeline.update(scans);

            if (scans.length === 0) {
                this.setStatus(`${site} — waiting for scans...`);
                return;
            }

            const latestChanged = scans[0].scan_time !== prevLatestScanTime;
            let targetIndex = prevIndex;
            if (forceLatest || prevCount === 0) {
                targetIndex = 0;
            } else if (prevDisplayedScanTime) {
                const remappedIndex = scans.findIndex((scan) => scan.scan_time === prevDisplayedScanTime);
                targetIndex = remappedIndex >= 0 ? remappedIndex : Math.min(prevIndex, scans.length - 1);
            } else {
                targetIndex = Math.min(prevIndex, scans.length - 1);
            }

            if (prevCount === 0) {
                this.log(`First scan set arrived — ${scans.length} cached`);
            } else if (latestChanged) {
                this.log(`Scan list updated — ${scans.length} cached`);
            }

            const shouldDisplay =
                forceLatest ||
                prevCount === 0 ||
                this.state.lastDisplayFailed ||
                (latestChanged && prevIndex === 0);

            if (shouldDisplay) {
                if (latestChanged && prevIndex === 0) {
                    targetIndex = 0;
                }
                await this.displayScan(targetIndex);
            } else if (targetIndex !== prevIndex) {
                this.state.currentScanIndex = targetIndex;
                Timeline.setCurrent(targetIndex);
            }

            this.setStatus(`${site} — ${scans.length} cached scan(s)`);
        } catch (err) {
            this.log(`refreshScans ERROR: ${err.message}`);
            console.error("Failed to refresh scans:", err);
        }
    },

    async displayScan(index) {
        const scan = this.state.scans[index];
        if (!scan) {
            this.log(`displayScan(${index}): no scan at this index`);
            return;
        }

        this.state.currentScanIndex = index;
        const site = this.state.activeSite;
        const product = this.state.activeProduct;
        const sweep = this.state.activeSweep;
        const scanTime = scan.scan_time;
        const encodedScanTime = encodeURIComponent(scanTime);

        this.log(`displayScan: ${site}/${scanTime}/${product}/${sweep}`);

        try {
            const metaUrl = `/api/radar/${site}/scan/${encodedScanTime}/${product}/${sweep}/meta`;
            const metaResp = await fetch(metaUrl);
            this.log(`meta response status: ${metaResp.status}`);

            if (!metaResp.ok) {
                const errText = await metaResp.text();
                this.state.lastDisplayFailed = true;
                this.log(`META FAILED: ${metaResp.status} — ${errText}`);
                return;
            }

            const meta = await metaResp.json();
            RadarMap.setRadarOverlay(meta.image_url, meta.bounds, site);
            this.state.lastDisplayFailed = false;

            Timeline.setCurrent(index);
            document.getElementById("timeline-label").textContent =
                `${site} — ${product} — ${new Date(scanTime).toLocaleTimeString()}`;
        } catch (err) {
            this.state.lastDisplayFailed = true;
            this.log(`displayScan ERROR: ${err.message}`);
            console.error("Failed to display scan:", err);
        }
    },

    onNewScan(data) {
        this.log(`WebSocket new_scan: ${data.site}`, data);
        if (data.site === this.state.activeSite) {
            this.refreshScans();
        }
    },
};

document.addEventListener("DOMContentLoaded", () => App.init());
