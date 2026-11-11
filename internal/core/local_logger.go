package core

import "context"

type loggerContextKey struct{}

// WithLogger sets an immutable logger override for this context only.
// nil clears the local override and resumes inheriting the global logger.
func WithLogger(ctx context.Context, l Logger) context.Context {
	return context.WithValue(ctx, loggerContextKey{}, &loggerHolder{l: l})
}

// LoggerForContext resolves a local logger before falling back to the global.
func LoggerForContext(ctx context.Context) Logger {
	if ctx != nil {
		if h, ok := ctx.Value(loggerContextKey{}).(*loggerHolder); ok && h.l != nil {
			return h.l
		}
	}
	return GetLogger()
}

// CopyLogger carries only the local logging setting to another context.
// Cancellation, deadlines and business values remain those of dst.
func CopyLogger(src, dst context.Context) context.Context {
	if src != nil {
		if h, ok := src.Value(loggerContextKey{}).(*loggerHolder); ok {
			return WithLogger(dst, h.l)
		}
	}
	return dst
}

// InheritLogger uses an instance logger only when the submitted work has no
// explicit scope of its own (for example work delegated by another builder).
func InheritLogger(src, dst context.Context) context.Context {
	if dst != nil {
		if _, ok := dst.Value(loggerContextKey{}).(*loggerHolder); ok {
			return dst
		}
	}
	return CopyLogger(src, dst)
}
