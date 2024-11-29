from __future__ import annotations

import reflex as rx


def page() -> rx.Component:
    # Welcome Page (Index)
    return rx.container(
        rx.color_mode.button(
            position="top-right",
        ),
        rx.vstack(
            rx.heading(
                "Welcome to Reflex!",
                size="9",
            ),
            rx.text(
                "Get started by editing ",
                size="5",
            ),
            rx.link(
                rx.button("Check out our docs!"),
                href="https://reflex.dev/docs/getting-started/introduction/",
                is_external=True,
            ),
            spacing="5",
            justify="center",
            min_height="85vh",
        ),
        rx.logo(),
    )
