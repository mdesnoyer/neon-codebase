from .responses import ELBResponse

url_bases = [
    "https?://elasticloadbalancing.(.+).amazonaws.com",
]

url_paths = {
    '{0}/$': ELBResponse().dispatch,
}
