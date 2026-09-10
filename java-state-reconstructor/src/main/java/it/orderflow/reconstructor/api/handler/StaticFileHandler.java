package it.orderflow.reconstructor.api.handler;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;

import java.io.IOException;
import java.io.InputStream;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Locale;
import java.util.Map;

public final class StaticFileHandler
    implements HttpHandler {

    private static final String STATIC_ROOT = "static";

    private static final Map<String, String> CONTENT_TYPES =
        Map.ofEntries(
            Map.entry("html", "text/html; charset=UTF-8"),
            Map.entry("css", "text/css; charset=UTF-8"),
            Map.entry(
                "js",
                "text/javascript; charset=UTF-8"
            ),
            Map.entry(
                "json",
                "application/json; charset=UTF-8"
            ),
            Map.entry("svg", "image/svg+xml"),
            Map.entry("png", "image/png"),
            Map.entry("jpg", "image/jpeg"),
            Map.entry("jpeg", "image/jpeg"),
            Map.entry("gif", "image/gif"),
            Map.entry("ico", "image/x-icon"),
            Map.entry("webp", "image/webp")
        );

    @Override
    public void handle(HttpExchange exchange)
        throws IOException {

        try {
            String method = exchange
                .getRequestMethod()
                .toUpperCase(Locale.ROOT);

            if (!"GET".equals(method)
                && !"HEAD".equals(method)) {

                exchange.getResponseHeaders().set(
                    "Allow",
                    "GET, HEAD"
                );

                sendText(
                    exchange,
                    405,
                    "Method not allowed"
                );
                return;
            }

            String resourcePath = resolveResourcePath(
                exchange.getRequestURI().getPath()
            );

            if (resourcePath == null) {
                sendText(
                    exchange,
                    400,
                    "Invalid resource path"
                );
                return;
            }

            try (
                InputStream resource =
                    getClass()
                        .getClassLoader()
                        .getResourceAsStream(resourcePath)
            ) {
                if (resource == null) {
                    sendText(
                        exchange,
                        404,
                        "Static resource not found"
                    );
                    return;
                }

                byte[] content = resource.readAllBytes();

                Headers headers =
                    exchange.getResponseHeaders();

                headers.set(
                    "Content-Type",
                    contentType(resourcePath)
                );
                headers.set(
                    "Cache-Control",
                    cacheControl(resourcePath)
                );
                headers.set(
                    "X-Content-Type-Options",
                    "nosniff"
                );

                if ("HEAD".equals(method)) {
                    headers.set(
                        "Content-Length",
                        String.valueOf(content.length)
                    );

                    exchange.sendResponseHeaders(
                        200,
                        -1
                    );
                    exchange.close();
                    return;
                }

                exchange.sendResponseHeaders(
                    200,
                    content.length
                );

                try (
                    var responseBody =
                        exchange.getResponseBody()
                ) {
                    responseBody.write(content);
                }
            }
        } catch (IllegalArgumentException exception) {
            sendText(
                exchange,
                400,
                "Invalid resource path"
            );
        }
    }

    private String resolveResourcePath(
        String requestPath
    ) {
        if (
            requestPath == null
                || requestPath.isBlank()
                || "/".equals(requestPath)
        ) {
            return STATIC_ROOT + "/index.html";
        }

        String decodedPath = URLDecoder.decode(
            requestPath,
            StandardCharsets.UTF_8
        );

        if (
            decodedPath.contains("..")
                || decodedPath.contains("\\")
        ) {
            return null;
        }

        while (decodedPath.startsWith("/")) {
            decodedPath = decodedPath.substring(1);
        }

        if (decodedPath.isBlank()) {
            return STATIC_ROOT + "/index.html";
        }

        if (decodedPath.endsWith("/")) {
            decodedPath += "index.html";
        }

        return STATIC_ROOT + "/" + decodedPath;
    }

    private String contentType(String resourcePath) {
        int extensionPosition =
            resourcePath.lastIndexOf('.');

        if (
            extensionPosition < 0
                || extensionPosition
                    == resourcePath.length() - 1
        ) {
            return "application/octet-stream";
        }

        String extension = resourcePath
            .substring(extensionPosition + 1)
            .toLowerCase(Locale.ROOT);

        return CONTENT_TYPES.getOrDefault(
            extension,
            "application/octet-stream"
        );
    }

    private String cacheControl(
        String resourcePath
    ) {
        if (resourcePath.endsWith(".html")) {
            return "no-cache";
        }

        return "public, max-age=3600";
    }

    private void sendText(
        HttpExchange exchange,
        int statusCode,
        String message
    ) throws IOException {
        byte[] content = message.getBytes(
            StandardCharsets.UTF_8
        );

        exchange.getResponseHeaders().set(
            "Content-Type",
            "text/plain; charset=UTF-8"
        );
        exchange.getResponseHeaders().set(
            "Cache-Control",
            "no-store"
        );
        exchange.getResponseHeaders().set(
            "X-Content-Type-Options",
            "nosniff"
        );

        exchange.sendResponseHeaders(
            statusCode,
            content.length
        );

        try (
            var responseBody =
                exchange.getResponseBody()
        ) {
            responseBody.write(content);
        }
    }
}