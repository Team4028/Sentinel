class ModalConfirm extends HTMLElement {
    constructor() {
        super();

        this.kage = this.attachShadow({ mode: "open" });
    }

    connectedCallback() {
        const buttonStyle = this.getAttribute('buttonStyle') ?? "";
        this.kage.innerHTML = `
        <link rel="stylesheet" href="${buttonStyle}">
        <style>
        .confirm-overlay {
            position: fixed;
            inset: 0;
            background: rgba(0, 0, 0, 0.5);
            display: none;
            align-items: center;
            justify-content: center;
            backdrop-filter: blur(5px);
        }

        .confirm-box {
            background: #333;
            padding: 20px;
            border-radius: 12px;
            color: var(--text);
            max-width: 90%;
            box-shadow: 0 10px 25px rgba(0, 0, 0, 0.2);
        }

        .confirm-box a {
            color: color-mix(in srgb, var(--accent), white 40%);
        }

        .confirm-actions {
            display: flex;
            justify-content: flex-end;
            gap: 10px;
            margin-top: 15px;
        }
        </style>
        <div id="confirmModal" class="confirm-overlay">
            <div class="confirm-box">
                <p id="confirmMessage"></p>
                <div class="confirm-actions">
                    <button class="sbutton" style="display: inline-block;" id="confirmCancel">Cancel</button>
                    <button class="sbutton" style="display: inline-block;" id="confirmOk">I understand, perform this action</button>
                </div>
            </div>
        </div>
        `;
    }

    async confirmModal(message, callbackIfYes) {
        if (await new Promise((resolve) => {
            const modal = this.kage.getElementById("confirmModal");
            const msg = this.kage.getElementById("confirmMessage");
            const okBtn = this.kage.getElementById("confirmOk");
            const cancelBtn = this.kage.getElementById("confirmCancel");

            msg.innerHTML = message;
            modal.style.display = 'flex';

            const cleanup = () => {
                modal.style.display = 'none';
                okBtn.removeEventListener("click", onOk);
                cancelBtn.removeEventListener("click", onCancel);
                modal.removeEventListener("click", onOutside);
                window.removeEventListener("keydown", onKey);
            };

            const onOk = () => {
                cleanup();
                resolve(true);
            };

            const onCancel = () => {
                cleanup();
                resolve(false);
            };

            const onOutside = (e) => {
                if (e.target === modal) onCancel();
            };

            const onKey = (e) => {
                if (e.key === "Escape") onCancel();
                if (e.key === "Enter") onOk();
            };

            okBtn.addEventListener("click", onOk);
            cancelBtn.addEventListener("click", onCancel);
            modal.addEventListener("click", onOutside);
            window.addEventListener("keydown", onKey);
        })) {
            callbackIfYes();
        }
    }
}

customElements.define('modal-confirm', ModalConfirm);