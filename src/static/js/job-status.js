class JobStatus extends HTMLElement {
    constructor() {
        super();

        this.kage = this.attachShadow({mode: "open"});
    }

    connectedCallback() {
        this.kage.innerHTML = `
            <style>
            .jobs {
                position: fixed;
                bottom: 10px;
                left: 10px;
                width: 250px;
                padding: 5px;
                border-radius: 5px;
                border: 1px solid #333;
                background: #222;
                color: #fff;
                z-index: 9999;
            }
            .jobs:empty {
                background: none;
                border: none;
            }
            </style>
            <div id='j*b-status' class='jobs'></div>
        `;

        setInterval(this.getUpdateEventStatuses.bind(this), 1000);
    }

    async getUpdateEventStatuses() {
        const res = await (await fetch("/jobs", {credentials: 'include'})).json();
        const container = this.kage.getElementById("j*b-status");
        container.innerHTML = "";
        for (const [name, status] of Object.entries(res)) {
            const wrapper = document.createElement("div");
            wrapper.style.marginBottom = "6px";

            const label = document.createElement("div");
            label.innerText = `${
                Object.keys(status).includes("error") ? "🔴" : (
                status["status"] === "done" ? "🟢" : (
                    status["status"] === "in-progress" ? "🟡" : (
                        status["status"] === "cancelled" ? "⚪" : "🔵"
                    )
                ))}${name} (${(status["prog"] * 100).toFixed(2)}%)`;
            label.style.marginBottom = "2px";

            const mini = document.createElement("h4");
            mini.style.color = "#666";
            mini.style.padding = 0;
            mini.style.margin = 0;
            mini.style.fontSize = 2;
            if (Object.keys(status).includes("error")) {
                mini.innerText = status["error"];
                mini.style.color = "#a00";
            } else
                mini.innerText = status["step"];

            const barOuter = document.createElement("div");
            barOuter.style.background = "#555";
            barOuter.style.width = "100%";
            barOuter.style.height = "16px";
            barOuter.style.borderRadius = "4px";
            barOuter.style.overflow = "hidden";

            const barInner = document.createElement("div");
            barInner.style.background = "#4caf50";
            barInner.style.width = `${status["prog"] * 100}%`;
            barInner.style.height = "100%";

            barOuter.appendChild(barInner);
            wrapper.appendChild(label);
            wrapper.appendChild(mini);
            wrapper.appendChild(barOuter);

            container.appendChild(wrapper);
        }
    }
}

customElements.define("job-status", JobStatus);