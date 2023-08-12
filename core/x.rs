mod types2 {
    use bytes::Bytes;
    use rkyv::{Archive, Deserialize, Serialize};
    #[archive(compare(PartialEq), check_bytes)]
    #[archive_attr(derive(Debug), repr(transparent))]
    #[repr(transparent)]
    pub struct Name(Vec<u8>);
    #[automatically_derived]
    ///An archived [`Name`]
    #[check_bytes(crate = "::rkyv::bytecheck")]
    #[repr(transparent)]
    pub struct ArchivedName(
        ///The archived counterpart of [`Name::0`]
        ::rkyv::Archived<Vec<u8>>,
    )
    where
        Vec<u8>: ::rkyv::Archive;
    #[automatically_derived]
    impl ::core::fmt::Debug for ArchivedName
    where
        Vec<u8>: ::rkyv::Archive,
    {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            ::core::fmt::Formatter::debug_tuple_field1_finish(f, "ArchivedName", &&self.0)
        }
    }
    #[allow(unused_results)]
    const _: () = {
        use ::core::{convert::Infallible, marker::PhantomData};
        use ::rkyv::bytecheck::{
            CheckBytes, EnumCheckError, ErrorBox, StructCheckError, TupleStructCheckError,
        };
        #[automatically_derived]
        impl<__C: ?Sized> CheckBytes<__C> for ArchivedName
        where
            Vec<u8>: ::rkyv::Archive,
            ::rkyv::Archived<Vec<u8>>: CheckBytes<__C>,
        {
            type Error = TupleStructCheckError;
            unsafe fn check_bytes<'__bytecheck>(
                value: *const Self,
                context: &mut __C,
            ) -> ::core::result::Result<&'__bytecheck Self, TupleStructCheckError> {
                let bytes = value.cast::<u8>();
                <::rkyv::Archived<Vec<u8>> as CheckBytes<__C>>::check_bytes(
                    &raw const (*value).0,
                    context,
                )
                .map_err(|e| TupleStructCheckError {
                    field_index: 0usize,
                    inner: ErrorBox::new(e),
                })?;
                Ok(&*value)
            }
        }
    };
    #[automatically_derived]
    ///The resolver for an archived [`Name`]
    pub struct NameResolver(::rkyv::Resolver<Vec<u8>>)
    where
        Vec<u8>: ::rkyv::Archive;
    #[automatically_derived]
    const _: () = {
        use ::core::marker::PhantomData;
        use ::rkyv::{out_field, Archive, Archived};
        impl Archive for Name
        where
            Vec<u8>: ::rkyv::Archive,
        {
            type Archived = ArchivedName;
            type Resolver = NameResolver;
            #[allow(clippy::unit_arg)]
            #[inline]
            unsafe fn resolve(
                &self,
                pos: usize,
                resolver: Self::Resolver,
                out: *mut Self::Archived,
            ) {
                let (fp, fo) = {
                    #[allow(unused_unsafe)]
                    unsafe {
                        let fo = &raw mut (*out).0;
                        (fo.cast::<u8>().offset_from(out.cast::<u8>()) as usize, fo)
                    }
                };
                ::rkyv::Archive::resolve((&self.0), pos + fp, resolver.0, fo);
            }
        }
        impl PartialEq<ArchivedName> for Name
        where
            Vec<u8>: ::rkyv::Archive,
            Archived<Vec<u8>>: PartialEq<Vec<u8>>,
        {
            #[inline]
            fn eq(&self, other: &ArchivedName) -> bool {
                true && other.0.eq(&self.0)
            }
        }
        impl PartialEq<Name> for ArchivedName
        where
            Vec<u8>: ::rkyv::Archive,
            Archived<Vec<u8>>: PartialEq<Vec<u8>>,
        {
            #[inline]
            fn eq(&self, other: &Name) -> bool {
                other.eq(self)
            }
        }
    };
    #[automatically_derived]
    const _: () = {
        use ::rkyv::{Archive, Archived, Deserialize, Fallible};
        impl<__D: Fallible + ?Sized> Deserialize<Name, __D> for Archived<Name>
        where
            Vec<u8>: Archive,
            Archived<Vec<u8>>: Deserialize<Vec<u8>, __D>,
        {
            #[inline]
            fn deserialize(
                &self,
                deserializer: &mut __D,
            ) -> ::core::result::Result<Name, __D::Error> {
                Ok(Name(Deserialize::<Vec<u8>, __D>::deserialize(
                    &self.0,
                    deserializer,
                )?))
            }
        }
    };
    #[automatically_derived]
    const _: () = {
        use ::rkyv::{Archive, Fallible, Serialize};
        impl<__S: Fallible + ?Sized> Serialize<__S> for Name
        where
            Vec<u8>: Serialize<__S>,
        {
            #[inline]
            fn serialize(
                &self,
                serializer: &mut __S,
            ) -> ::core::result::Result<Self::Resolver, __S::Error> {
                Ok(NameResolver(Serialize::<__S>::serialize(
                    &self.0, serializer,
                )?))
            }
        }
    };
    #[automatically_derived]
    impl ::core::fmt::Debug for Name {
        fn fmt(&self, f: &mut ::core::fmt::Formatter) -> ::core::fmt::Result {
            ::core::fmt::Formatter::debug_tuple_field1_finish(f, "Name", &&self.0)
        }
    }
    #[automatically_derived]
    impl ::core::marker::StructuralPartialEq for Name {}
    #[automatically_derived]
    impl ::core::cmp::PartialEq for Name {
        #[inline]
        fn eq(&self, other: &Name) -> bool {
            self.0 == other.0
        }
    }
    #[automatically_derived]
    impl ::core::clone::Clone for Name {
        #[inline]
        fn clone(&self) -> Name {
            Name(::core::clone::Clone::clone(&self.0))
        }
    }
}
