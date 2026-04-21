class UserDisplay extends HTMLElement {
    constructor() {
        super();
        this.kage = this.attachShadow({ mode: "open" });
    }

    connectedCallback() {
        let id = null;
        if ((id = this.getAttribute("id")) === null) return;
        fetch("get-user-display", {
            method: "POST",
            headers: {
                "id": id
            },
            credentials: "include",
        }).then(async r => {
            if (r.ok) {
                this.kage.innerHTML = await r.text();
            }
        })
    }
}

customElements.define("user-disp", UserDisplay);