use std::{
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use futures_util::{future, Never};
use http::{Request, Response};
use tonic::{body::BoxBody, transport::Body};
use tower::Service;

use rpc::service::server::MayastorServer;

use crate::{
    csi::server::{IdentityServer, NodeServer},
    identity::Identity,
    mayastor_svc::MayastorService,
    node::Node,
};

#[derive(Clone)]
pub struct Router {
    pub node_service: Arc<Node>,
    pub identity_service: Arc<Identity>,
    pub mayastor_service: Arc<MayastorService>,
}

impl Service<()> for Router {
    type Response = Router;
    type Error = Never;
    type Future = future::Ready<Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Ok(()).into()
    }

    fn call(&mut self, _req: ()) -> Self::Future {
        future::ok(self.clone())
    }
}

impl Service<Request<Body>> for Router {
    type Response = Response<BoxBody>;
    type Error = Never;
    type Future = Pin<
        Box<
            dyn Future<Output = Result<Response<BoxBody>, Never>>
                + Send
                + 'static,
        >,
    >;

    fn poll_ready(
        &mut self,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), Self::Error>> {
        Ok(()).into()
    }

    fn call(&mut self, req: Request<Body>) -> Self::Future {
        let mut segments = req.uri().path().split('/');
        segments.next();
        let service = segments.next().unwrap();
        match service {
            "csi.v1.Identity" => {
                let me = self.clone();
                Box::pin(async move {
                    let mut svc =
                        IdentityServer::from_shared(me.identity_service);

                    let mut svc = svc.call(()).await.unwrap();

                    let res = svc.call(req).await.unwrap();
                    Ok(res)
                })
            }

            "csi.v1.Node" => {
                let me = self.clone();
                Box::pin(async move {
                    let mut svc = NodeServer::from_shared(me.node_service);
                    let mut svc = svc.call(()).await.unwrap();

                    let res = svc.call(req).await.unwrap();
                    Ok(res)
                })
            }
            "mayastor_service.Mayastor" => {
                let me = self.clone();
                Box::pin(async move {
                    let mut svc =
                        MayastorServer::from_shared(me.mayastor_service);
                    let mut svc = svc.call(()).await.unwrap();

                    let res = svc.call(req).await.unwrap();
                    Ok(res)
                })
            }

            _ => unimplemented!(),
        }
    }
}
