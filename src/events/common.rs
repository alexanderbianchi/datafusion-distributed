use datafusion::execution::config::SessionConfig;
use std::sync::Arc;

pub(crate) struct EventHandlerChain<H: ?Sized> {
    built_in: Vec<Arc<H>>,
    custom: Vec<Arc<H>>,
}

impl<H: ?Sized> Default for EventHandlerChain<H> {
    fn default() -> Self {
        Self {
            built_in: Vec::new(),
            custom: Vec::new(),
        }
    }
}

impl<H: ?Sized> Clone for EventHandlerChain<H> {
    fn clone(&self) -> Self {
        Self {
            built_in: self.built_in.clone(),
            custom: self.custom.clone(),
        }
    }
}

impl<H: ?Sized> EventHandlerChain<H> {
    pub(super) fn find_map<T>(&self, mut f: impl FnMut(&H) -> Option<T>) -> Option<T> {
        if let Some(res) = self.custom.iter().find_map(|handler| f(handler.as_ref())) {
            return Some(res);
        }
        self.built_in.iter().find_map(|handler| f(handler.as_ref()))
    }
}

impl<H: ?Sized + Send + Sync + 'static> EventHandlerChain<H> {
    pub(crate) fn register_built_in(cfg: &mut SessionConfig, handler: Arc<H>) {
        let mut handlers = cfg
            .get_extension::<Self>()
            .map(|v| v.as_ref().clone())
            .unwrap_or_default();
        handlers.built_in.push(handler);
        cfg.set_extension(Arc::new(handlers));
    }

    pub(crate) fn register_custom(cfg: &mut SessionConfig, handler: Arc<H>) {
        let mut handlers = cfg
            .get_extension::<Self>()
            .map(|v| v.as_ref().clone())
            .unwrap_or_default();
        handlers.custom.push(handler);
        cfg.set_extension(Arc::new(handlers));
    }
}
