'use strict';

let cid = crypto.randomUUID();
let curr_interval = null;

function checkNotifications() {
    fetch("/notifyq", {
        method: "GET",
        credentials: 'include',
        headers: {
            'Content-Type': 'application/json',
            'X-Cid': cid
        }
    }).then(async res => {
        if (res.ok && res.status === 200) { // do nothing if 204
            const json = await res.json();
            console.log(`SEND ${JSON.stringify(json)}`);
            if ("title" in json) {
                const title = json["title"];
                if (title === "TBA Webhook verification") {
                    const body = json["body"] ?? "";
                    const cls = await clients.matchAll({type: 'window'});
                    for (const client of cls) {
                        client.postMessage({
                            type: "COPY_CLIP",
                            text: body
                        });
                    }
                    self.registration.showNotification("Clipboard", {
                        body: `Copied text ${body} to clipboard`,
                        icon: json["icon"] ?? ""
                    });
                }
                delete json.title;
                self.registration.showNotification(title, json);
            }
        }
    }).catch((e) => { console.log(`Error: ${e}`) });
}

self.addEventListener('install', function () {
    console.log('Service Worker installing.');
});

self.addEventListener('activate', async function () {
    console.log('Service Worker activating.');
});

self.addEventListener("message", async function (m) {
    console.log(`SW -- ${m.data}`)
    if (Object.keys(m.data).includes("data") && m.data.data === "Wake Up") {
        if (curr_interval) {
            clearInterval(curr_interval);
            curr_interval = null;
        }
        let js = await(await fetch("/我是谁")).json();
        if (js["logged_in"] && js["admin"])
            curr_interval = setInterval(checkNotifications, 1000);
    }
});

self.addEventListener("notificationclick", event => {
    console.log(`Clicked ${event.action}`); // goto-changes or remove-change, defined in app.py in the webpush data
    if (event.action === "goto-changes") { // go to /changes endpoint:
        event.waitUntil(
            clients.matchAll({ type: "window" }).then(cList => {
                for (const client of cList) {
                    // if /changes is already open, go to that page and reload it
                    if (client.url === new URL('/changes', self.location.href).toString() && "focus" in client) {
                        client.navigate(client.url);
                        return client.focus();
                    }
                }
                // else, open /changes
                if (clients.openWindow) return clients.openWindow("/changes");
            })
        );
    } else if (event.action === "remove-change") {
        fetch("/delete-lines", { // use stored notification data from webpush in flask to delete these lines
            method: "POST",
            credentials: 'include',
            headers: {
                "Content-Type": "application/json",
            },
            "body": JSON.stringify({ lines: JSON.parse(event.notification.data["line-hashes"]) }) // parse and then unparse the json string
        }).then(res => {
            if (res.status === 401 || res.status === 403) {
                self.registration.showNotification("Error", {
                    body: "Authentication error: are you logged in?"
                });
            } else if (res.status === 500) {
                res.text().then(txt => {
                    self.registration.showNotification("Error", {
                        body: `Internal Server error: ${txt}`
                    })
                })
            }
        });
    }
});