const WS = {
    socket: null,
    reconnectDelay: 1000,
    maxReconnectDelay: 30000,

    init() {
        this.connect();
    },

    connect() {
        const protocol = window.location.protocol === "https:" ? "wss:" : "ws:";
        const url = `${protocol}//${window.location.host}/api/radar/ws`;
        this.socket = new WebSocket(url);

        this.socket.onopen = () => {
            console.log("WebSocket connected.");
            this.reconnectDelay = 1000;
            App.setStatus("Connected");
        };

        this.socket.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                this.handleMessage(data);
            } catch (err) {
                console.error("WebSocket message parse error:", err);
            }
        };

        this.socket.onclose = () => {
            console.log(`WebSocket closed. Reconnecting in ${this.reconnectDelay}ms...`);
            setTimeout(() => this.connect(), this.reconnectDelay);
            this.reconnectDelay = Math.min(
                this.reconnectDelay * 2,
                this.maxReconnectDelay
            );
        };

        this.socket.onerror = (err) => {
            console.error("WebSocket error:", err);
        };
    },

    handleMessage(data) {
        switch (data.type) {
            case "new_scan":
                console.log(`New scan: ${data.site} at ${data.scan_time}`);
                App.onNewScan(data);
                break;
            default:
                console.log("Unknown WS message:", data);
        }
    },
};
