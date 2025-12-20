'use strict';

let csrfToken = null;

self.addEventListener("message", event => {
    if (event.data.type == "SET_CSRF") {
        csrfToken = event.data.token;
        console.log("Service Worker recieved CSRF");
    }
});

self.addEventListener('install', function (event) {
    console.log('Service Worker installing.');
});

self.addEventListener('activate', function (event) {
    console.log('Service Worker activating.');
});

self.addEventListener("notificationclick", event => {
    console.log(`Clicked ${event.action}`); // goto-changes or remove-change, defined in app.py in the webpush data
    if (event.action === "goto-changes") { // go to /changes endpoint:
        event.waitUntil(
            clients.matchAll({type: "window"}).then(cList => {
                for (const client of cList) {
                    console.log(client);
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
                'X-CSRFToken': csrfToken
            },
            "body": JSON.stringify({ lines: JSON.parse(event.notification.data["line-hashes"]) }) // parse and then unparse the json string
        });
    }
});

self.addEventListener('push', function (event) {
    console.log('[Service Worker] Push Received.');
    const pushData = event.data.text();
    console.log(`[Service Worker] Push received this data - "${pushData}"`);
    let data, title, body, icon, actions, nData;
    // read all of the notification params, make sure the bare minimum is there
    try {
        data = JSON.parse(pushData);
        title = data.title;
        body = data.body;
        icon = data.icon;
        actions = data.actions;
        if (data.data)
            nData = data.data;
    } catch (e) {
        title = "Untitled";
        body = pushData;
    }
    const options = {
        body: body,
        icon: icon,
        actions: actions,
    };
    if (nData) options["data"] = nData;
    console.log(title, options);

    event.waitUntil(
        self.registration.showNotification(title, options) // make and send the notification
    );
});