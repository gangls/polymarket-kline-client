import { logger } from "../utils/logger";

const DEFAULT_GAMMA_EVENTS_URL = "https://gamma-api.polymarket.com/events/keyset?active=true&closed=false";
const DEFAULT_REQUIRED_TAG_IDS = ["1312", "101757", "102169", "21"];
const DEFAULT_MARKET_TYPE_TAG_IDS = ["102127"];
const DEFAULT_INTERVAL_TAG_IDS = ["102892", "102467", "102175", "102531", "102281"];
const DEFAULT_ASSET_TAG_IDS = ["235", "39", "818", "101267", "100178", "102331", "102716"];
const HOURLY_TAG_ID = "102175";
const DAILY_TAG_ID = "102281";
const DEFAULT_QUERY_TAG_IDS = ["102892", "102467", "102531"];
const DEFAULT_PAGE_SIZE = 50;
const DEFAULT_FETCH_RETRIES = 3;
const CURRENT_WINDOW_LOOKBACK_MS = 5 * 60 * 60 * 1000;
const CURRENT_WINDOW_LOOKAHEAD_MS = 70 * 60 * 1000;
const HOURLY_ASSET_SLUGS = ["bitcoin", "ethereum", "solana", "xrp", "dogecoin", "hype", "bnb"];
const DAILY_UP_DOWN_ASSET_SLUGS = ["bitcoin", "ethereum", "solana", "xrp", "dogecoin", "hype", "bnb"];

export interface DiscoveredMarket {
    id?: string;
    slug?: string;
    eventSlug?: string;
    question?: string;
    conditionId?: string;
    tagIds: string[];
    assetIds: string[];
    acceptingOrders?: boolean;
    enableOrderBook?: boolean;
    eventStartTime?: string;
    endDate?: string;
}

export interface MarketDiscoveryConfig {
    gammaEventsUrl: string;
    requiredTagIds: string[];
    marketTypeTagIds: string[];
    intervalTagIds: string[];
    assetTagIds: string[];
    pageSize: number;
    maxEvents: number;
    liveOnly: boolean;
    apiLive: boolean;
    currentOnly: boolean;
    queryTagIds: string[];
    hourlySlugDiscovery: boolean;
    dailySlugDiscovery: boolean;
}

interface GammaTag {
    id?: string | number;
    slug?: string;
    label?: string;
}

interface GammaMarket {
    id?: string;
    slug?: string;
    question?: string;
    conditionId?: string;
    condition_id?: string;
    clobTokenIds?: string[] | string;
    clob_token_ids?: string[] | string;
    tags?: GammaTag[];
    active?: boolean;
    closed?: boolean;
    archived?: boolean;
    enableOrderBook?: boolean;
    acceptingOrders?: boolean;
    eventStartTime?: string;
    startDate?: string;
    endDate?: string;
}

interface GammaEvent {
    id?: string;
    ticker?: string;
    slug?: string;
    title?: string;
    tags?: GammaTag[];
    markets?: GammaMarket[];
    active?: boolean;
    closed?: boolean;
    archived?: boolean;
    enableOrderBook?: boolean;
}

export class MarketDiscoveryService {
    private readonly config: MarketDiscoveryConfig;

    constructor(config?: Partial<MarketDiscoveryConfig>) {
        this.config = {
            gammaEventsUrl: process.env.GAMMA_EVENTS_URL || DEFAULT_GAMMA_EVENTS_URL,
            requiredTagIds: parseCsv(process.env.MARKET_REQUIRED_TAG_IDS, DEFAULT_REQUIRED_TAG_IDS),
            marketTypeTagIds: parseCsv(process.env.MARKET_TYPE_TAG_IDS, DEFAULT_MARKET_TYPE_TAG_IDS),
            intervalTagIds: parseCsv(process.env.MARKET_INTERVAL_TAG_IDS, DEFAULT_INTERVAL_TAG_IDS),
            assetTagIds: parseCsv(process.env.MARKET_ASSET_TAG_IDS, DEFAULT_ASSET_TAG_IDS),
            pageSize: Number(process.env.MARKET_DISCOVERY_PAGE_SIZE) || DEFAULT_PAGE_SIZE,
            maxEvents: Number(process.env.MARKET_DISCOVERY_MAX_EVENTS) || 0,
            liveOnly: process.env.MARKET_DISCOVERY_LIVE_ONLY !== "false",
            apiLive: process.env.MARKET_DISCOVERY_API_LIVE === "true",
            currentOnly: process.env.MARKET_DISCOVERY_CURRENT_ONLY !== "false",
            queryTagIds: parseCsv(
                process.env.MARKET_DISCOVERY_QUERY_TAG_IDS || process.env.MARKET_DISCOVERY_QUERY_TAG_ID,
                DEFAULT_QUERY_TAG_IDS,
            ),
            hourlySlugDiscovery: process.env.MARKET_DISCOVERY_HOURLY_SLUG_DISCOVERY !== "false",
            dailySlugDiscovery: process.env.MARKET_DISCOVERY_DAILY_SLUG_DISCOVERY !== "false",
            ...config,
        };
    }

    async discoverCryptoUpDownMarkets(): Promise<DiscoveredMarket[]> {
        const events = await this.fetchEvents();
        const markets = new Map<string, DiscoveredMarket>();

        for (const event of events) {
            if (!isUpDownEvent(event)) continue;

            const eventTagIds = getTagIds(event.tags);
            for (const market of event.markets || []) {
                const tagIds = unique([...eventTagIds, ...getTagIds(market.tags)]);
                if (!containsAll(tagIds, this.config.requiredTagIds)) continue;
                if (!containsAny(tagIds, this.config.marketTypeTagIds)) continue;
                if (!isAllowedInterval(event, market, tagIds, this.config.intervalTagIds)) continue;
                if (!containsAny(tagIds, this.config.assetTagIds)) continue;
                if (this.config.liveOnly && !isLiveMarket(event, market)) continue;
                if (this.config.currentOnly && !isCurrentMarket(market)) continue;

                const assetIds = parseTokenIds(market.clobTokenIds ?? market.clob_token_ids);
                if (assetIds.length === 0) continue;

                const key = market.conditionId || market.condition_id || market.slug || market.id || assetIds.join(":");
                markets.set(key, {
                    id: market.id,
                    slug: market.slug || event.slug,
                    eventSlug: event.slug,
                    question: market.question || event.title,
                    conditionId: market.conditionId || market.condition_id,
                    tagIds,
                    assetIds,
                    acceptingOrders: market.acceptingOrders,
                    enableOrderBook: market.enableOrderBook,
                    eventStartTime: market.eventStartTime || market.startDate,
                    endDate: market.endDate,
                });
            }
        }

        const discovered = Array.from(markets.values());
        logger.info(
            `Discovered ${discovered.length} crypto up/down markets requiredTags=${this.config.requiredTagIds.join(",")} marketTypeTags=${this.config.marketTypeTagIds.join(",")} intervalTags=${this.config.intervalTagIds.join(",")} assetTags=${this.config.assetTagIds.join(",")} liveOnly=${this.config.liveOnly} currentOnly=${this.config.currentOnly}`,
        );
        return discovered;
    }

    async discoverCryptoUpDownAssetIds(): Promise<string[]> {
        const markets = await this.discoverCryptoUpDownMarkets();
        return unique(markets.flatMap(market => market.assetIds));
    }

    private async fetchEvents(): Promise<GammaEvent[]> {
        const eventsByKey = new Map<string, GammaEvent>();
        let totalPageCount = 0;
        for (const queryTagId of this.config.queryTagIds) {
            let cursor = "";

            while (true) {
                const url = new URL(this.config.gammaEventsUrl);
                url.searchParams.set("limit", String(this.config.pageSize));
                if (this.config.currentOnly) {
                    url.searchParams.set(
                        "start_time_min",
                        new Date(Date.now() - CURRENT_WINDOW_LOOKBACK_MS).toISOString(),
                    );
                    url.searchParams.set(
                        "start_time_max",
                        new Date(Date.now() + CURRENT_WINDOW_LOOKAHEAD_MS).toISOString(),
                    );
                }
                if (this.config.apiLive && !url.searchParams.has("live")) {
                    url.searchParams.set("live", "true");
                }
                if (queryTagId && !url.searchParams.has("tag_id")) {
                    url.searchParams.set("tag_id", queryTagId);
                }
                if (cursor) {
                    url.searchParams.set("after_cursor", cursor);
                }

                const body = await fetchJsonWithRetry(url);
                const page = normalizeEventsResponse(body);
                if (page.length === 0) break;
                totalPageCount++;

                for (const event of page) {
                    const key = event.id || event.slug || JSON.stringify(event);
                    eventsByKey.set(key, event);
                    if (this.config.maxEvents > 0 && eventsByKey.size >= this.config.maxEvents) break;
                }

                if (page.length < this.config.pageSize) break;
                if (this.config.maxEvents > 0 && eventsByKey.size >= this.config.maxEvents) break;

                cursor = getNextCursor(body);
                if (!cursor) break;
            }

            if (this.config.maxEvents > 0 && eventsByKey.size >= this.config.maxEvents) break;
        }

        if (this.config.hourlySlugDiscovery && (this.config.maxEvents === 0 || eventsByKey.size < this.config.maxEvents)) {
            const hourlyEvents = await this.fetchHourlyEventsBySlug();
            for (const event of hourlyEvents) {
                const key = event.id || event.slug || JSON.stringify(event);
                eventsByKey.set(key, event);
                if (this.config.maxEvents > 0 && eventsByKey.size >= this.config.maxEvents) break;
            }
        }

        if (this.config.dailySlugDiscovery && (this.config.maxEvents === 0 || eventsByKey.size < this.config.maxEvents)) {
            const dailyEvents = await this.fetchDailyEventsBySlug();
            for (const event of dailyEvents) {
                const key = event.id || event.slug || JSON.stringify(event);
                eventsByKey.set(key, event);
                if (this.config.maxEvents > 0 && eventsByKey.size >= this.config.maxEvents) break;
            }
        }

        const events = Array.from(eventsByKey.values());
        logger.info(
            `Fetched ${events.length} active Gamma events across ${totalPageCount} pages for market discovery queryTags=${this.config.queryTagIds.join(",")}`,
        );
        return events;
    }

    private async fetchHourlyEventsBySlug(): Promise<GammaEvent[]> {
        const slugs = currentHourlySlugs();
        const events: GammaEvent[] = [];

        for (const slug of slugs) {
            try {
                const url = eventSlugUrl(this.config.gammaEventsUrl, slug);
                const body = await fetchJsonWithRetry(url);
                const event = normalizeEventResponse(body);
                if (event) events.push(event);
            } catch (error) {
                logger.warn(`Gamma hourly event fetch failed slug=${slug}:`, error);
            }
        }

        logger.info(`Fetched ${events.length}/${slugs.length} hourly Gamma events by slug`);
        return events;
    }

    private async fetchDailyEventsBySlug(): Promise<GammaEvent[]> {
        const slugs = currentDailySlugs();
        const events: GammaEvent[] = [];

        for (const slug of slugs) {
            try {
                const url = eventSlugUrl(this.config.gammaEventsUrl, slug);
                const body = await fetchJsonWithRetry(url);
                const event = normalizeEventResponse(body);
                if (event) events.push(event);
            } catch (error) {
                logger.warn(`Gamma daily event fetch failed slug=${slug}:`, error);
            }
        }

        logger.info(`Fetched ${events.length}/${slugs.length} daily Gamma events by slug`);
        return events;
    }
}

function normalizeEventsResponse(body: unknown): GammaEvent[] {
    if (Array.isArray(body)) return body as GammaEvent[];
    if (body && typeof body === "object" && Array.isArray((body as { events?: unknown }).events)) {
        return (body as { events: GammaEvent[] }).events;
    }
    return [];
}

function normalizeEventResponse(body: unknown): GammaEvent | null {
    return body && typeof body === "object" && !Array.isArray(body) ? (body as GammaEvent) : null;
}

function getNextCursor(body: unknown): string {
    if (!body || typeof body !== "object") return "";
    const cursor = (body as { next_cursor?: unknown }).next_cursor;
    return typeof cursor === "string" ? cursor : "";
}

async function fetchJsonWithRetry(url: URL): Promise<unknown> {
    let lastError: unknown;

    for (let attempt = 1; attempt <= DEFAULT_FETCH_RETRIES; attempt++) {
        try {
            const response = await fetch(url, {
                headers: {
                    "accept": "application/json",
                    "accept-encoding": "identity",
                },
            });
            if (!response.ok) {
                throw new Error(`Gamma events request failed: ${response.status} ${response.statusText}`);
            }
            return await response.json();
        } catch (error) {
            lastError = error;
            logger.warn(
                `Gamma events fetch failed (attempt ${attempt}/${DEFAULT_FETCH_RETRIES}) url=${url.toString()}:`,
                error,
            );
            if (attempt < DEFAULT_FETCH_RETRIES) {
                await delay(500 * attempt);
            }
        }
    }

    throw lastError;
}

function parseTokenIds(value: unknown): string[] {
    if (Array.isArray(value)) return value.map(String).filter(Boolean);
    if (typeof value !== "string" || value.length === 0) return [];

    try {
        const parsed = JSON.parse(value);
        return Array.isArray(parsed) ? parsed.map(String).filter(Boolean) : [];
    } catch {
        return [value];
    }
}

function getTagIds(tags: GammaTag[] | undefined): string[] {
    return (tags || []).map(tag => String(tag.id || "")).filter(Boolean);
}

function containsAll(values: string[], required: string[]): boolean {
    const valueSet = new Set(values);
    return required.every(value => valueSet.has(value));
}

function containsAny(values: string[], candidates: string[]): boolean {
    if (candidates.length === 0) return true;
    const valueSet = new Set(values);
    return candidates.some(value => valueSet.has(value));
}

function isAllowedInterval(
    event: GammaEvent,
    market: GammaMarket,
    tagIds: string[],
    intervalTagIds: string[],
): boolean {
    if (containsAny(tagIds, intervalTagIds)) return true;
    return isFourHourMarket(event, market) || isHourlyMarket(event, market, tagIds) || isDailyMarket(tagIds);
}

function isFourHourMarket(event: GammaEvent, market: GammaMarket): boolean {
    return marketText(event, market).includes("updown-4h");
}

function isHourlyMarket(event: GammaEvent, market: GammaMarket, tagIds: string[]): boolean {
    const text = marketText(event, market);
    return tagIds.includes(HOURLY_TAG_ID) || /-\d{1,2}(am|pm)-et\b/.test(text);
}

function isDailyMarket(tagIds: string[]): boolean {
    return tagIds.includes(DAILY_TAG_ID);
}

function isUpDownEvent(event: GammaEvent): boolean {
    const text = `${event.slug || ""} ${event.title || ""} ${event.ticker || ""}`.toLowerCase();
    return text.includes("up-or-down") || text.includes("updown") || text.includes("up or down");
}

function marketText(event: GammaEvent, market: GammaMarket): string {
    return `${event.slug || ""} ${event.title || ""} ${market.slug || ""} ${market.question || ""}`.toLowerCase();
}

function isLiveMarket(event: GammaEvent, market: GammaMarket): boolean {
    return (
        event.active !== false &&
        event.closed !== true &&
        event.archived !== true &&
        market.active !== false &&
        market.closed !== true &&
        market.archived !== true &&
        market.enableOrderBook === true &&
        market.acceptingOrders === true
    );
}

function isCurrentMarket(market: GammaMarket): boolean {
    const startTime = parseDateMs(market.eventStartTime || market.startDate);
    const endTime = parseDateMs(market.endDate);
    const now = Date.now();

    if (!startTime || !endTime) return false;
    return startTime <= now && now < endTime;
}

function parseDateMs(value: string | undefined): number {
    if (!value) return 0;
    const timestamp = Date.parse(value);
    return Number.isNaN(timestamp) ? 0 : timestamp;
}

function parseCsv(value: string | undefined, fallback: string[]): string[] {
    const parsed = (value || "")
        .split(",")
        .map(item => item.trim())
        .filter(Boolean);
    return parsed.length > 0 ? parsed : fallback;
}

function unique(values: string[]): string[] {
    return Array.from(new Set(values));
}

function delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
}

function currentHourlySlugs(): string[] {
    const now = new Date();
    const normalHour = easternSlugTime(now);

    return HOURLY_ASSET_SLUGS.map(asset => `${asset}-up-or-down-${normalHour}`);
}

function currentDailySlugs(): string[] {
    const date = easternSlugDate(new Date());

    return DAILY_UP_DOWN_ASSET_SLUGS.map(asset => `${asset}-up-or-down-on-${date.withYear}`);
}

function easternSlugTime(date: Date): string {
    const parts = new Intl.DateTimeFormat("en-US", {
        timeZone: "America/New_York",
        year: "numeric",
        month: "long",
        day: "numeric",
        hour: "numeric",
        hour12: true,
    }).formatToParts(date);

    const value = (type: string): string => parts.find(part => part.type === type)?.value || "";
    const month = value("month").toLowerCase();
    const day = value("day");
    const year = value("year");
    const hour = value("hour");
    const period = value("dayPeriod").toLowerCase();

    return `${month}-${day}-${year}-${hour}${period}-et`;
}

function easternSlugDate(date: Date): { withYear: string } {
    const parts = new Intl.DateTimeFormat("en-US", {
        timeZone: "America/New_York",
        year: "numeric",
        month: "long",
        day: "numeric",
    }).formatToParts(date);

    const value = (type: string): string => parts.find(part => part.type === type)?.value || "";
    const month = value("month").toLowerCase();
    const day = value("day");
    const year = value("year");

    return {
        withYear: `${month}-${day}-${year}`,
    };
}

function eventSlugUrl(gammaEventsUrl: string, slug: string): URL {
    const url = new URL(gammaEventsUrl);
    return new URL(`/events/slug/${slug}`, url.origin);
}
