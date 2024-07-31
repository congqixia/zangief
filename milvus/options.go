package milvus

func WithRestfulPath(path string) MilvusOption {
	return func(opt *option) {
		opt.restfulPath = path
	}
}

func WithToken(token string) MilvusOption {
	return func(opt *option) {
		opt.token = token
	}
}

func WithPassword(username string) MilvusOption {
	return func(opt *option) {
		opt.username = username
	}
}

func WithUsername(password string) MilvusOption {
	return func(opt *option) {
		opt.password = password
	}
}

func WithPooling(pooling bool) MilvusOption {
	return func(opt *option) {
		opt.pooling = pooling
	}
}

func WithCustomTransport(custom bool) MilvusOption {
	return func(opt *option) {
		opt.customTransport = custom
	}
}

func WithMaxIdleConnsPerHost(maxIdleConnsPerHost int) MilvusOption {
	return func(opt *option) {
		opt.maxIdleConnsPerHost = maxIdleConnsPerHost
	}
}

func WithMaxConnsPerHost(maxConnsPerHost int) MilvusOption {
	return func(opt *option) {
		opt.maxConnsPerHost = maxConnsPerHost
	}
}

func WithForceAttempHTTP2(forceAttemptH2 bool) MilvusOption {
	return func(opt *option) {
		opt.forceAttemptH2 = forceAttemptH2
	}
}

func WithDisableKeepAlives(disableKeepAlives bool) MilvusOption {
	return func(opt *option) {
		opt.disableKeepAlives = disableKeepAlives
	}
}

type MilvusOption func(opt *option)

func defaultOption() *option {
	return &option{
		restfulPath: "/v1/vector/search",
		pooling:     true,
	}
}
