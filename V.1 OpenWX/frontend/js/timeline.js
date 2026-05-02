const Timeline = {
    scans: [],
    playInterval: null,

    init() {
        document.getElementById("btn-prev").addEventListener("click", () => this.prev());
        document.getElementById("btn-play").addEventListener("click", () => this.togglePlay());
        document.getElementById("btn-next").addEventListener("click", () => this.next());

        document.getElementById("speed-slider").addEventListener("input", (e) => {
            if (App.state.playing) {
                this.stopPlay();
                this.startPlay();
            }
        });

        document.getElementById("timeline-bar").addEventListener("click", (e) => {
            if (this.scans.length === 0) return;
            const bar = e.currentTarget;
            const rect = bar.getBoundingClientRect();
            const pct = (e.clientX - rect.left) / rect.width;
            const index = Math.round(pct * (this.scans.length - 1));
            App.displayScan(index);
        });
    },

    update(scans) {
        this.scans = scans;
        const thumb = document.getElementById("timeline-thumb");
        if (scans.length > 0) {
            thumb.classList.remove("hidden");
        }
    },

    setCurrent(index) {
        App.state.currentScanIndex = index;
        const pct = this.scans.length > 1
            ? (index / (this.scans.length - 1)) * 100
            : 0;
        document.getElementById("timeline-track").style.width = `${pct}%`;
        const thumb = document.getElementById("timeline-thumb");
        thumb.style.left = `${pct}%`;
    },

    prev() {
        if (this.scans.length === 0) return;
        const newIndex = Math.max(0, App.state.currentScanIndex - 1);
        App.displayScan(newIndex);
    },

    next() {
        if (this.scans.length === 0) return;
        const newIndex = (App.state.currentScanIndex + 1) % this.scans.length;
        App.displayScan(newIndex);
    },

    togglePlay() {
        if (App.state.playing) {
            this.stopPlay();
        } else {
            this.startPlay();
        }
    },

    startPlay() {
        if (this.scans.length === 0) return;
        App.state.playing = true;
        document.getElementById("btn-play").innerHTML = "&#9646;&#9646;";
        const speed = document.getElementById("speed-slider").value;
        const intervalMs = 1100 - (speed * 100);
        this.playInterval = setInterval(() => this.next(), intervalMs);
    },

    stopPlay() {
        App.state.playing = false;
        document.getElementById("btn-play").innerHTML = "&#9654;";
        if (this.playInterval) {
            clearInterval(this.playInterval);
            this.playInterval = null;
        }
    },
};
