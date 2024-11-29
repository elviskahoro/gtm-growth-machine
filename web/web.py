from __future__ import annotations

import reflex as rx

from web.pages.index import page

app = rx.App()
app.add_page(
    component=page,
    route="/",
    title="title",
    description="description",
    image="favicon",
    on_load=None,
    meta=[
        {
            "author": "elvis kahoro",
        },
    ],
)
