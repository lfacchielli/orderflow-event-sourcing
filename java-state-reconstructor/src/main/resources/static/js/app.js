const state = {
    health: null,
    orders: [],
    filteredOrders: [],
    selectedOrderId: null,
    selectedOrder: null,
    snapshots: [],
    historicalState: null
};

const elements = {
    healthIndicator:
        document.getElementById("health-indicator"),
    healthText:
        document.getElementById("health-text"),
    refreshButton:
        document.getElementById("refresh-button"),
    errorMessage:
        document.getElementById("error-message"),
    totalOrders:
        document.getElementById("total-orders"),
    deliveredOrders:
        document.getElementById("delivered-orders"),
    delayedOrders:
        document.getElementById("delayed-orders"),
    averageDelay:
        document.getElementById("average-delay"),
    ordersCaption:
        document.getElementById("orders-caption"),
    orderSearch:
        document.getElementById("order-search"),
    statusFilter:
        document.getElementById("status-filter"),
    ordersBody:
        document.getElementById("orders-body"),
    drawerOverlay:
        document.getElementById("drawer-overlay"),
    drawerOrderId:
        document.getElementById("drawer-order-id"),
    drawerContent:
        document.getElementById("drawer-content"),
    closeDrawer:
        document.getElementById("close-drawer")
};

async function requestJson(url) {
    const response = await fetch(url, {
        headers: {
            Accept: "application/json"
        }
    });

    const body = await response.json();

    if (!response.ok) {
        throw new Error(
            body.message
                || `HTTP request failed: ${response.status}`
        );
    }

    return body;
}

async function loadDashboard() {
    setLoading(true);
    clearError();

    try {
        const [health, orderResponse] =
            await Promise.all([
                requestJson("/api/health"),
                requestJson("/api/orders")
            ]);

        state.health = health;
        state.orders = orderResponse.orders || [];

        populateStatusFilter();
        applyFilters();
        renderHealth();
        renderMetrics();
    } catch (error) {
        showError(error.message);
        renderUnavailableHealth();
    } finally {
        setLoading(false);
    }
}

function setLoading(loading) {
    elements.refreshButton.disabled = loading;
    elements.refreshButton.textContent =
        loading ? "Caricamento..." : "Aggiorna";
}

function renderHealth() {
    const healthy =
        state.health
        && state.health.status === "UP"
        && state.health.database === "UP";

    elements.healthIndicator.classList.remove(
        "health-loading",
        "health-up",
        "health-down"
    );

    elements.healthIndicator.classList.add(
        healthy ? "health-up" : "health-down"
    );

    elements.healthText.textContent = healthy
        ? "Sistema operativo"
        : "Sistema degradato";
}

function renderUnavailableHealth() {
    elements.healthIndicator.classList.remove(
        "health-loading",
        "health-up"
    );
    elements.healthIndicator.classList.add(
        "health-down"
    );
    elements.healthText.textContent =
        "Sistema non raggiungibile";
}

function renderMetrics() {
    const delivered = state.orders.filter(
        order => order.status === "DELIVERED"
    ).length;

    const delayed = state.orders.filter(
        order => order.hasDelay
    ).length;

    const totalDelay = state.orders.reduce(
        (total, order) =>
            total + Number(order.totalDelayMinutes || 0),
        0
    );

    const averageDelay = state.orders.length === 0
        ? 0
        : Math.round(totalDelay / state.orders.length);

    elements.totalOrders.textContent =
        state.orders.length;

    elements.deliveredOrders.textContent =
        delivered;

    elements.delayedOrders.textContent =
        delayed;

    elements.averageDelay.textContent =
        `${averageDelay} min`;
}

function populateStatusFilter() {
    const selectedValue =
        elements.statusFilter.value;

    const statuses = [...new Set(
        state.orders.map(order => order.status)
    )].sort();

    elements.statusFilter.innerHTML =
        '<option value="ALL">Tutti gli stati</option>';

    statuses.forEach(status => {
        const option = document.createElement("option");
        option.value = status;
        option.textContent =
            formatStatus(status);

        elements.statusFilter.appendChild(option);
    });

    if (
        [...elements.statusFilter.options]
            .some(option => option.value === selectedValue)
    ) {
        elements.statusFilter.value = selectedValue;
    }
}

function applyFilters() {
    const query =
        elements.orderSearch.value
            .trim()
            .toLowerCase();

    const selectedStatus =
        elements.statusFilter.value;

    state.filteredOrders = state.orders.filter(order => {
        const searchableValues = [
            order.orderId,
            order.customerId,
            order.destinationCity,
            order.currentHub
        ];

        const matchesQuery =
            query.length === 0
            || searchableValues.some(value =>
                String(value || "")
                    .toLowerCase()
                    .includes(query)
            );

        const matchesStatus =
            selectedStatus === "ALL"
            || order.status === selectedStatus;

        return matchesQuery && matchesStatus;
    });

    renderOrders();
}

function renderOrders() {
    elements.ordersCaption.textContent =
        `${state.filteredOrders.length} ordini visualizzati`;

    if (state.filteredOrders.length === 0) {
        elements.ordersBody.innerHTML = `
            <tr>
                <td colspan="7" class="empty-cell">
                    Nessun ordine corrisponde ai filtri.
                </td>
            </tr>
        `;

        return;
    }

    elements.ordersBody.innerHTML =
        state.filteredOrders
            .map(order => `
                <tr data-order-id="${escapeHtml(order.orderId)}">
                    <td>
                        <div class="order-id">
                            ${escapeHtml(order.orderId)}
                        </div>
                        <div class="order-secondary">
                            ${escapeHtml(order.customerId)}
                        </div>
                    </td>

                    <td>
                        ${statusBadge(order.status)}
                    </td>

                    <td>
                        ${escapeHtml(order.destinationCity)}
                    </td>

                    <td>
                        ${escapeHtml(
                            order.currentHub || "Non assegnato"
                        )}
                    </td>

                    <td>
                        <span class="${
                            order.hasDelay
                                ? "badge badge-warning"
                                : "badge badge-neutral"
                        }">
                            ${Number(
                                order.totalDelayMinutes || 0
                            )} min
                        </span>
                    </td>

                    <td>
                        <strong>
                            ${formatAmount(order.totalAmount)}
                            ${escapeHtml(order.currency)}
                        </strong>
                    </td>

                    <td>
                        <span class="badge badge-neutral">
                            v${Number(order.version)}
                        </span>
                    </td>
                </tr>
            `)
            .join("");

    elements.ordersBody
        .querySelectorAll("[data-order-id]")
        .forEach(row => {
            row.addEventListener("click", () => {
                openOrder(row.dataset.orderId);
            });
        });
}

async function openOrder(orderId) {
    state.selectedOrderId = orderId;
    state.selectedOrder = null;
    state.snapshots = [];
    state.historicalState = null;

    elements.drawerOrderId.textContent = orderId;
    elements.drawerContent.innerHTML =
        "<p>Caricamento dettaglio...</p>";

    elements.drawerOverlay.classList.remove("hidden");

    try {
        const [order, snapshotResponse] =
            await Promise.all([
                requestJson(
                    `/api/orders/${encodeURIComponent(orderId)}`
                ),
                requestJson(
                    `/api/orders/${
                        encodeURIComponent(orderId)
                    }/snapshots`
                )
            ]);

        state.selectedOrder = order;
        state.snapshots =
            snapshotResponse.snapshots || [];

        renderOrderDrawer();
    } catch (error) {
        elements.drawerContent.innerHTML = `
            <div class="error-message">
                ${escapeHtml(error.message)}
            </div>
        `;
    }
}

function closeDrawer() {
    elements.drawerOverlay.classList.add("hidden");
    state.selectedOrderId = null;
    state.selectedOrder = null;
    state.snapshots = [];
    state.historicalState = null;
}

function renderOrderDrawer() {
    const order =
        state.historicalState
            ? state.historicalState.state
            : state.selectedOrder;

    if (!order) {
        return;
    }

    const historicalBanner = state.historicalState
        ? `
            <div class="historical-banner">
                <span>
                    Stato storico alla versione
                    <strong>
                        ${state.historicalState.requestedVersion}
                    </strong>
                </span>

                <button
                    id="current-state-button"
                    class="button button-secondary"
                    type="button"
                >
                    Stato corrente
                </button>
            </div>
        `
        : "";

    const items = (order.items || [])
        .map(item => `
            <div class="detail-item">
                <span class="detail-label">
                    ${escapeHtml(item.productId)}
                </span>
                <div class="detail-value">
                    ${escapeHtml(item.productName)}
                </div>
                <div class="order-secondary">
                    Quantità ${Number(item.quantity)}
                    · ${formatAmount(item.unitPrice)}
                    ${escapeHtml(order.currency)}
                </div>
            </div>
        `)
        .join("");

    const hubs = (order.visitedHubs || [])
        .map(hub => `
            <span class="hub">
                ${escapeHtml(hub)}
            </span>
        `)
        .join("");

    const snapshots = state.snapshots.length === 0
        ? "<p>Nessuno snapshot disponibile.</p>"
        : state.snapshots
            .map(snapshot => `
                <button
                    class="snapshot-button"
                    type="button"
                    data-snapshot-version="${snapshot.version}"
                >
                    <span>
                        <strong>
                            Versione ${snapshot.version}
                        </strong>
                        <span class="order-secondary">
                            ${formatDate(
                                snapshot.snapshotCreatedAt
                            )}
                        </span>
                    </span>

                    ${statusBadge(snapshot.status)}
                </button>
            `)
            .join("");

    elements.drawerContent.innerHTML = `
        ${historicalBanner}

        <section class="detail-section">
            <h3>Stato</h3>

            <div class="detail-grid">
                <div class="detail-item">
                    <span class="detail-label">
                        Stato corrente
                    </span>
                    <div>
                        ${statusBadge(order.status)}
                    </div>
                </div>

                <div class="detail-item">
                    <span class="detail-label">
                        Versione
                    </span>
                    <div class="detail-value">
                        v${Number(order.version)}
                    </div>
                </div>

                <div class="detail-item">
                    <span class="detail-label">
                        Cliente
                    </span>
                    <div class="detail-value">
                        ${escapeHtml(order.customerId)}
                    </div>
                </div>

                <div class="detail-item">
                    <span class="detail-label">
                        Importo
                    </span>
                    <div class="detail-value">
                        ${formatAmount(order.totalAmount)}
                        ${escapeHtml(order.currency)}
                    </div>
                </div>
            </div>
        </section>

        <section class="detail-section">
            <h3>Logistica</h3>

            <div class="detail-grid">
                <div class="detail-item">
                    <span class="detail-label">
                        Destinazione
                    </span>
                    <div class="detail-value">
                        ${escapeHtml(
                            order.destination?.city || ""
                        )},
                        ${escapeHtml(
                            order.destination?.country || ""
                        )}
                    </div>
                </div>

                <div class="detail-item">
                    <span class="detail-label">
                        Hub corrente
                    </span>
                    <div class="detail-value">
                        ${escapeHtml(
                            order.currentHub || "Non assegnato"
                        )}
                    </div>
                </div>

                <div class="detail-item">
                    <span class="detail-label">
                        Ritardo totale
                    </span>
                    <div class="detail-value">
                        ${Number(
                            order.totalDelayMinutes || 0
                        )} minuti
                    </div>
                </div>

                <div class="detail-item">
                    <span class="detail-label">
                        Ultimo aggiornamento
                    </span>
                    <div class="detail-value">
                        ${formatDate(order.lastUpdatedAt)}
                    </div>
                </div>
            </div>

            <h3 style="margin-top: 18px;">
                Hub visitati
            </h3>

            <div class="hub-list">
                ${hubs || "<p>Nessun hub visitato.</p>"}
            </div>
        </section>

        <section class="detail-section">
            <h3>Articoli</h3>

            <div class="item-list">
                ${items || "<p>Nessun articolo.</p>"}
            </div>
        </section>

        <section class="detail-section">
            <h3>Snapshot disponibili</h3>

            <div class="snapshot-list">
                ${snapshots}
            </div>
        </section>
    `;

    elements.drawerContent
        .querySelectorAll("[data-snapshot-version]")
        .forEach(button => {
            button.addEventListener("click", () => {
                loadHistoricalState(
                    state.selectedOrderId,
                    button.dataset.snapshotVersion
                );
            });
        });

    const currentStateButton =
        document.getElementById("current-state-button");

    if (currentStateButton) {
        currentStateButton.addEventListener(
            "click",
            () => {
                state.historicalState = null;
                renderOrderDrawer();
            }
        );
    }
}

async function loadHistoricalState(
    orderId,
    version
) {
    try {
        state.historicalState = await requestJson(
            `/api/orders/${
                encodeURIComponent(orderId)
            }/state?version=${
                encodeURIComponent(version)
            }`
        );

        renderOrderDrawer();
    } catch (error) {
        showError(error.message);
    }
}

function statusBadge(status) {
    const normalizedStatus = String(
        status || "UNKNOWN"
    );

    let className = "badge-neutral";

    if (normalizedStatus === "DELIVERED") {
        className = "badge-delivered";
    } else if (
        normalizedStatus === "PACKED"
        || normalizedStatus === "CREATED"
    ) {
        className = "badge-packed";
    } else if (
        normalizedStatus === "IN_TRANSIT"
        || normalizedStatus === "OUT_FOR_DELIVERY"
    ) {
        className = "badge-transit";
    } else if (
        normalizedStatus.includes("FAILED")
        || normalizedStatus === "CANCELLED"
    ) {
        className = "badge-warning";
    }

    return `
        <span class="badge ${className}">
            ${escapeHtml(formatStatus(normalizedStatus))}
        </span>
    `;
}

function formatStatus(status) {
    return String(status || "UNKNOWN")
        .replaceAll("_", " ");
}

function formatAmount(value) {
    const number = Number(value || 0);

    return new Intl.NumberFormat(
        "it-IT",
        {
            minimumFractionDigits: 2,
            maximumFractionDigits: 2
        }
    ).format(number);
}

function formatDate(value) {
    if (!value) {
        return "Non disponibile";
    }

    return new Intl.DateTimeFormat(
        "it-IT",
        {
            dateStyle: "short",
            timeStyle: "short"
        }
    ).format(new Date(value));
}

function escapeHtml(value) {
    return String(value ?? "")
        .replaceAll("&", "&amp;")
        .replaceAll("<", "&lt;")
        .replaceAll(">", "&gt;")
        .replaceAll('"', "&quot;")
        .replaceAll("'", "&#039;");
}

function showError(message) {
    elements.errorMessage.textContent = message;
    elements.errorMessage.classList.remove("hidden");
}

function clearError() {
    elements.errorMessage.textContent = "";
    elements.errorMessage.classList.add("hidden");
}

elements.refreshButton.addEventListener(
    "click",
    loadDashboard
);

elements.orderSearch.addEventListener(
    "input",
    applyFilters
);

elements.statusFilter.addEventListener(
    "change",
    applyFilters
);

elements.closeDrawer.addEventListener(
    "click",
    closeDrawer
);

elements.drawerOverlay.addEventListener(
    "click",
    event => {
        if (event.target === elements.drawerOverlay) {
            closeDrawer();
        }
    }
);

document.addEventListener(
    "keydown",
    event => {
        if (event.key === "Escape") {
            closeDrawer();
        }
    }
);

loadDashboard();