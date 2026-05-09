/* http_client.h — thin libcurl wrapper used by the Flight server to talk to
 * the DataFlow OS gateway. Synchronous; one client instance is fine to
 * share across requests since libcurl handles are per-call. */
#pragma once
#include <string>

namespace dfo {

struct HttpResponse {
    int         status = 0;     // -1 on transport error
    std::string body;
    std::string error;
};

class HttpClient {
public:
    HttpClient(std::string base_url, std::string bearer);
    ~HttpClient();

    HttpResponse get   (const std::string& path) const;
    HttpResponse post_json(const std::string& path,
                           const std::string& json_body) const;
    HttpResponse post_raw (const std::string& path,
                           const std::string& body,
                           const std::string& content_type) const;

private:
    std::string base_;
    std::string bearer_;
};

}  // namespace dfo
