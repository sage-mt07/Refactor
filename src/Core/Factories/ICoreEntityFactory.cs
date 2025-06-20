﻿using KsqlDsl.Core.Abstractions;
using System;
using System.Collections.Generic;

namespace KsqlDsl.Core.Factories
{
    public interface ICoreEntityFactory
    {
        EntityModel CreateEntityModel<T>() where T : class;
        EntityModel CreateEntityModel(Type entityType);
        bool CanCreateEntityModel(Type entityType);
        List<Type> GetSupportedEntityTypes();
    }
}
