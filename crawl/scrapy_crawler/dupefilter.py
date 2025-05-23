from scrapy.dupefilters import RFPDupeFilter

class EntranceAwareDupeFilter(RFPDupeFilter):
    def request_fingerprint(self, request):
        # 获取原始指纹
        fp = super().request_fingerprint(request)
        # 获取入口URL（从请求的meta中）
        entrance_url = request.meta.get('base_url', '')
        # 将入口URL加入指纹计算
        return f"{fp}:{entrance_url}"